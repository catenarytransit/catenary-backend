use actix_web::{HttpResponse, Responder, web};
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen_dataset::AspenisedAlert;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct QueryAlertInfo {
    pub chateau: String,
}

#[derive(Serialize, Deserialize)]
struct AlertsResponse {
    pub alerts: HashMap<String, AspenisedAlert>,
    pub routes: HashMap<String, catenary::models::Route>,
    pub stops: HashMap<String, catenary::SerializableStop>,
}

#[actix_web::get("/fetchalertsofchateau/")]
pub async fn fetch_alerts_with_hydrated_data(
    query: web::Query<QueryAlertInfo>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
    let etcd =
        catenary::get_etcd_client(&etcd_connection_ips, &etcd_connection_options, &etcd_reuser)
            .await;

    if etcd.is_err() {
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", query.chateau).as_str(),
            None,
        )
        .await;

    if let Err(err_fetch) = &fetch_assigned_node {
        eprintln!("{}", err_fetch);
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Error fetching assigned node from etcd");
    }

    let fetch_assigned_node = fetch_assigned_node.unwrap();

    if fetch_assigned_node.kvs().is_empty() {
        return HttpResponse::Ok()
            .append_header(("Cache-Control", "no-cache"))
            .body("No assigned node found for this chateau");
    }

    let assigned_chateau_data = catenary::bincode_deserialize::<ChateauMetadataEtcd>(
        fetch_assigned_node.kvs().first().unwrap().value(),
    )
    .unwrap();

    let aspen_client =
        catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket).await;

    if aspen_client.is_err() {
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Error connecting to assigned node via tarpc");
    }

    let aspen_client = aspen_client.unwrap();

    let alerts_response = aspen_client
        .get_all_alerts(tarpc::context::current(), query.chateau.clone())
        .await;

    match alerts_response {
        Ok(Some(alerts)) => {
            let mut route_ids = HashSet::new();
            let mut stop_ids = HashSet::new();

            for alert in alerts.values() {
                for entity in &alert.informed_entity {
                    if let Some(route_id) = &entity.route_id {
                        route_ids.insert(route_id.clone());
                    }
                    if let Some(stop_id) = &entity.stop_id {
                        stop_ids.insert(stop_id.clone());
                    }
                }
            }

            let conn_pool = pool.as_ref();
            let conn_pre = conn_pool.get().await;
            let conn = &mut conn_pre.unwrap();

            let routes_map: HashMap<String, catenary::models::Route> = if !route_ids.is_empty() {
                let routes_vec: Vec<catenary::models::Route> =
                    catenary::schema::gtfs::routes::dsl::routes
                        .filter(catenary::schema::gtfs::routes::chateau.eq(&query.chateau))
                        .filter(catenary::schema::gtfs::routes::route_id.eq_any(&route_ids))
                        .select(catenary::models::Route::as_select())
                        .load(conn)
                        .await
                        .unwrap_or_default();

                routes_vec
                    .into_iter()
                    .map(|r| (r.route_id.clone(), r))
                    .collect()
            } else {
                HashMap::new()
            };

            let stops_map: HashMap<String, catenary::SerializableStop> = if !stop_ids.is_empty() {
                let stops_vec: Vec<catenary::models::Stop> =
                    catenary::schema::gtfs::stops::dsl::stops
                        .filter(catenary::schema::gtfs::stops::chateau.eq(&query.chateau))
                        .filter(catenary::schema::gtfs::stops::gtfs_id.eq_any(&stop_ids))
                        .select(catenary::models::Stop::as_select())
                        .load(conn)
                        .await
                        .unwrap_or_default();

                stops_vec
                    .into_iter()
                    .map(|s| {
                        let (lon, lat) = match &s.point {
                            Some(p) => (Some(p.x), Some(p.y)),
                            None => (None, None),
                        };
                        (
                            s.gtfs_id.clone(),
                            catenary::SerializableStop {
                                id: s.gtfs_id,
                                code: s.code,
                                name: s.name,
                                description: s.gtfs_desc,
                                location_type: s.location_type,
                                parent_station: s.parent_station,
                                zone_id: s.zone_id,
                                longitude: lon,
                                latitude: lat,
                                timezone: s.timezone,
                                platform_code: s.platform_code,
                                level_id: s.level_id,
                                routes: s.routes.into_iter().flatten().collect(),
                            },
                        )
                    })
                    .collect()
            } else {
                HashMap::new()
            };

            let response = AlertsResponse {
                alerts,
                routes: routes_map,
                stops: stops_map,
            };

            HttpResponse::Ok()
                .append_header(("Content-Type", "application/json"))
                .json(response)
        }
        Ok(None) => HttpResponse::Ok()
            .append_header(("Content-Type", "application/json"))
            .json(AlertsResponse {
                alerts: HashMap::new(),
                routes: HashMap::new(),
                stops: HashMap::new(),
            }),
        Err(e) => {
            eprintln!("Error fetching alerts: {:?}", e);
            HttpResponse::InternalServerError()
                .append_header(("Cache-Control", "no-cache"))
                .body("Error fetching alerts from aspen")
        }
    }
}

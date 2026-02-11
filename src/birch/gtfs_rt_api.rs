use crate::EtcdConnectionIps;
use actix_web::{HttpResponse, Responder, web};
use catenary::aspen_dataset::GtfsRtType;
use catenary::get_node_for_realtime_feed_id;
use prost::Message;
use serde::Deserialize;
use std::io::Read;
use std::sync::Arc;

#[derive(Deserialize, Clone)]
struct BirchGtfsRtOptions {
    feed_id: String,
    feed_type: String,
    format: Option<String>,
}

enum ConvertedFormat {
    Protobuf,
    Json,
    Ron,
}

#[actix_web::get("/gtfs_rt")]
async fn gtfs_rt(
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    query: web::Query<BirchGtfsRtOptions>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
    let query = query.into_inner();

    let etcd =
        catenary::get_etcd_client(&etcd_connection_ips, &etcd_connection_options, &etcd_reuser)
            .await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_meta = get_node_for_realtime_feed_id(&mut etcd, &query.feed_id).await;

    match fetch_assigned_node_meta {
        Some(data) => {
            let worker_id = data.worker_id;

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket)
                .await
                .unwrap();

            let feed_type = match query.feed_type.as_str() {
                "vehicle" => Some(GtfsRtType::VehiclePositions),
                "trip" => Some(GtfsRtType::TripUpdates),
                "alert" => Some(GtfsRtType::Alerts),
                _ => None,
            };

            let format = match &query.format {
                Some(format_gtfs_req) => match format_gtfs_req.as_str() {
                    "pb" => ConvertedFormat::Protobuf,
                    "json" => ConvertedFormat::Json,
                    "ron" => ConvertedFormat::Ron,
                    _ => ConvertedFormat::Protobuf,
                },
                _ => ConvertedFormat::Protobuf,
            };

            match feed_type {
                Some(feed_type) => {
                    let get_gtfs = aspen_client
                        .get_gtfs_rt_compressed(tarpc::context::current(), query.feed_id, feed_type)
                        .await;

                    match get_gtfs {
                        Err(_) => HttpResponse::InternalServerError()
                            .append_header(("Cache-Control", "no-cache"))
                            .body("Node crashed during request"),
                        Ok(Some(protobuf)) => {
                            let decompressed_bytes = catenary::decompress_zlib(protobuf.as_slice());

                            match catenary::parse_gtfs_rt_message(decompressed_bytes.as_slice()) {
                                Ok(data) => match format {
                                    ConvertedFormat::Protobuf => HttpResponse::Ok()
                                        .append_header(("Cache-Control", "no-cache"))
                                        .body(data.encode_to_vec()),
                                    ConvertedFormat::Ron => HttpResponse::Ok()
                                        .append_header(("Cache-Control", "no-cache"))
                                        .append_header(actix_web::http::header::ContentType(
                                            mime::TEXT_PLAIN,
                                        ))
                                        .body(
                                            ron::ser::to_string_pretty(
                                                &data,
                                                ron::ser::PrettyConfig::default(),
                                            )
                                            .unwrap(),
                                        ),
                                    ConvertedFormat::Json => HttpResponse::Ok()
                                        .append_header(("Cache-Control", "no-cache"))
                                        .append_header(actix_web::http::header::ContentType(
                                            mime::APPLICATION_JSON,
                                        ))
                                        .body(serde_json::to_string_pretty(&data).unwrap()),
                                },
                                Err(_) => HttpResponse::InternalServerError()
                                    .append_header(("Cache-Control", "no-cache"))
                                    .body("Failed to decode protobuf data"),
                            }
                        }
                        Ok(None) => HttpResponse::InternalServerError()
                            .append_header(("Cache-Control", "no-cache"))
                            .body("Data doesn't exist on node. try again in a few minutes?"),
                    }
                }
                None => HttpResponse::NotFound()
                    .append_header(("Cache-Control", "no-cache"))
                    .body("Bad Feed Type, either vehicle trip or alert accepted"),
            }
        }
        None => {
            return HttpResponse::InternalServerError()
                .append_header(("Cache-Control", "no-cache"))
                .body("Could not find Assigned Node");
        }
    }
}

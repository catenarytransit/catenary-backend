use ahash::AHashMap;
use ecow::EcoString;
use gtfs_structures::Gtfs;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};

const LIRR_TRIPS_FEED: &str =
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr";

const MNR_TRIPS_FEED: &str =
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr";

pub async fn fetch_mta_lirr_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    client: &reqwest::Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    /*
    let fetch_url =
        "https://backend-unified.mylirr.org/locations?geometry=TRACK_TURF&railroad=LIRR";

    let request = client
        .get(fetch_url)
        .header("Accept-Version", "3.0")
        .send()
        .await;
     */

    let gtfs_rt_trips = get_mta_trips(client, LIRR_TRIPS_FEED).await;

    if let Ok(gtfs_rt_trips) = gtfs_rt_trips {
        let lirr_trip_updates_bytes = gtfs_rt_trips.encode_to_vec();

        send_mta_rail_to_aspen(
            etcd,
            MtaRailroad::LIRR,
            lirr_trip_updates_bytes.clone(),
            lirr_trip_updates_bytes,
            feed_id,
        )
        .await;
    }

    Ok(())
}

pub async fn fetch_mta_metronorth_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    client: &reqwest::Client,
    mnr_gtfs: &gtfs_structures::Gtfs,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let fetch_url = "https://backend-unified.mylirr.org/locations?geometry=TRACK_TURF&railroad=MNR";

    let request = client
        .get(fetch_url)
        .header("Accept-Version", "3.0")
        .send()
        .await;

    let gtfs_rt_trips = get_mta_trips(client, MNR_TRIPS_FEED).await;

    if let Ok(request) = request {
        if let Ok(gtfs_rt_trips) = gtfs_rt_trips {
            let mnr_trip_id_fixed = mnr_trip_id_fixer(mnr_gtfs, gtfs_rt_trips);

            let body = request.text().await.unwrap();

            let import_data = serde_json::from_str::<Vec<MtaTrain>>(body.as_str());

            if let Ok(import_data) = import_data {
                let converted = convert(&import_data, MtaRailroad::MNR, &mnr_trip_id_fixed);

                let mnr_vehicle_position = catenary::make_feed_from_entity_vec(converted);

                let mnr_vehicle_position_bytes = mnr_vehicle_position.encode_to_vec();
                let mnr_trip_updates_bytes = mnr_trip_id_fixed.encode_to_vec();

                send_mta_rail_to_aspen(
                    etcd,
                    MtaRailroad::MNR,
                    mnr_vehicle_position_bytes,
                    mnr_trip_updates_bytes,
                    feed_id,
                )
                .await;
            }
        }
    }

    Ok(())
}

fn get_lirr_train_id(entity: &gtfs_realtime::FeedEntity) -> String {
    let mut train_id = String::from("");

    if entity.vehicle.is_some() {
        let vehicle = entity.vehicle.as_ref().unwrap();

        if vehicle.trip.is_some() {
            let trip = vehicle.trip.as_ref().unwrap();

            if trip.trip_id.is_some() {
                let pre_train_id = trip.trip_id.as_ref().unwrap().clone();

                //split by underscore

                let split: Vec<&str> = pre_train_id.split('_').collect();

                //get last element

                train_id = String::from(split[split.len() - 1]);
            }
        }
    }

    train_id
}

async fn get_mta_trips(
    client: &reqwest::Client,
    url: &str,
) -> Result<gtfs_realtime::FeedMessage, Box<dyn std::error::Error + Sync + Send>> {
    let bytes = client
        .get(url)
        //exposed on purpose. Not my key, this is from the MTA
        .header("x-api-key", "hvThsOlHmP2XzvYWlKKC17YPcq07meIg2V2RPLbC")
        .send()
        .await?
        .bytes()
        .await?
        .to_vec();

    let decoded: gtfs_realtime::FeedMessage = gtfs_realtime::FeedMessage::decode(bytes.as_slice())?;

    Ok(decoded)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainStatus {
    otp: Option<i32>,
    otp_location: Option<String>,
    held: bool,
    canceled: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainCar {
    #[serde(rename = "type")]
    traintype: String,
    number: Option<i32>,
    loading: String,
    restroom: Option<bool>,
    revenue: Option<bool>,
    bikes: Option<i32>,
    locomotive: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainConsist {
    cars: Vec<TrainCar>,
    fleet: Option<String>,
    actual_len: Option<i32>,
    sched_len: Option<i32>,
    occupancy: Option<String>,
    occupancy_timestamp: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainLocation {
    longitude: f32,
    latitude: f32,
    //recieved in miles per hour, needs conversion to meters per second
    speed: Option<f32>,
    heading: Option<f32>,
    source: String,
    timestamp: i32,
    extra_info: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainTurf {
    length: f32,
    location_mp: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainStop {
    code: String,
    sched_time: i32,
    sign_track: Option<String>,
    avps_track_id: Option<String>,
    posted: bool,
    t2s_track: String,
    stop_status: Option<String>,
    stop_type: String,
    track_change: Option<bool>,
    local_cancel: Option<bool>,
    bus: bool,
    occupancy: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainDetails {
    headsign: String,
    summary: String,
    peak_code: String,
    branch: Option<String>,
    stops: Vec<TrainStop>,
    direction: String,
    turf: Option<TrainTurf>,
    //"PERMITTED" or "PROHIBITED"
    bike_rule: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MtaTrain {
    train_id: String,
    //MNR or LIRR
    railroad: String,
    run_date: String,
    train_num: String,
    realtime: bool,
    details: TrainDetails,
    consist: TrainConsist,
    location: TrainLocation,
    status: TrainStatus,
}

struct MetroNorthLookupTables {}

fn lookup_tables(gtfs: &Gtfs) -> AHashMap<EcoString, BTreeSet<EcoString>> {
    let mut trip_name_to_ids: AHashMap<EcoString, BTreeSet<EcoString>> = AHashMap::new();

    for (trip_id, trip) in gtfs.trips.iter() {
        if let Some(short_name) = &trip.trip_short_name {
            trip_name_to_ids
                .entry(short_name.clone().into())
                .and_modify(|x| {
                    x.insert(trip_id.clone().into());
                })
                .or_insert(BTreeSet::from_iter([trip_id.clone().into()]));
        }
    }

    trip_name_to_ids
}

fn mnr_trip_id_fixer(gtfs: &Gtfs, input: gtfs_realtime::FeedMessage) -> gtfs_realtime::FeedMessage {
    let lookup_tables_made = lookup_tables(gtfs);

    let mut input = input;

    for entity in input.entity.iter_mut() {
        if let Some(trip_update) = &mut entity.trip_update {
            if let Some(original_trip_id) = &mut trip_update.trip.trip_id {
                //checks if the trip id given by Metro North is actually invalid
                if !gtfs.trips.contains_key(original_trip_id) {
                    //it's invalid.

                    //get the operating trip short name, which is contained in the vehicle id of the gtfs field
                    if let Some(vehicle_data) = &mut entity.vehicle {
                        if let Some(vehicle) = &vehicle_data.vehicle {
                            if let Some(vehicle_id) = &vehicle.id {
                                if let Some(schedule_trip_ids_with_same_name) =
                                    lookup_tables_made.get(vehicle_id.as_str())
                                {
                                    //println!("MTA MNR metro north lookup {} found {} trips", vehicle_id, schedule_trip_ids_with_same_name.len());
                                    match schedule_trip_ids_with_same_name.len() {
                                        0 => {}
                                        1 => {
                                            *original_trip_id = schedule_trip_ids_with_same_name
                                                .iter()
                                                .nth(0)
                                                .unwrap()
                                                .to_string();

                                            println!(
                                                "Changed to {}",
                                                schedule_trip_ids_with_same_name
                                                    .iter()
                                                    .nth(0)
                                                    .unwrap()
                                                    .to_string()
                                            );

                                            if let Some(trip_vehicle_desc) = &mut vehicle_data.trip
                                            {
                                                trip_vehicle_desc.trip_id = Some(
                                                    schedule_trip_ids_with_same_name
                                                        .iter()
                                                        .nth(0)
                                                        .unwrap()
                                                        .to_string(),
                                                );
                                            }
                                        }
                                        _ => {
                                            let naive_date = trip_update
                                                .trip
                                                .start_date
                                                .as_ref()
                                                .map(|x| {
                                                    catenary::yyyymmdd_to_naive_date(x.as_str())
                                                        .ok()
                                                })
                                                .flatten();

                                            if let Some(naive_date) = naive_date {
                                                let trip_id_found =
                                                    schedule_trip_ids_with_same_name.iter().find(
                                                        |proposed_trip_id| {
                                                            if let Some(trip) = gtfs
                                                                .trips
                                                                .get(proposed_trip_id.as_str())
                                                            {
                                                                if let Some(calendar_dates) = gtfs
                                                                    .calendar_dates
                                                                    .get(trip.service_id.as_str())
                                                                {
                                                                    let naive_date_list =
                                                                        calendar_dates
                                                                            .iter()
                                                                            .map(|each_cal| {
                                                                                each_cal.date
                                                                            })
                                                                            .collect::<Vec<_>>();

                                                                    if naive_date_list
                                                                        .contains(&naive_date)
                                                                    {
                                                                        return true;
                                                                    }
                                                                }
                                                            }

                                                            false
                                                        },
                                                    );

                                                if let Some(trip_id_found) = trip_id_found {
                                                    *original_trip_id = trip_id_found.to_string();

                                                    if let Some(trip_vehicle_desc) =
                                                        &mut vehicle_data.trip
                                                    {
                                                        trip_vehicle_desc.trip_id =
                                                            Some(trip_id_found.to_string());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    input
}

fn convert(
    mta: &Vec<MtaTrain>,
    railroad: MtaRailroad,
    input_gtfs_trips: &gtfs_realtime::FeedMessage,
) -> Vec<gtfs_realtime::FeedEntity> {
    let railroad_str = match railroad {
        MtaRailroad::LIRR => "LIRR",
        MtaRailroad::MNR => "MNR",
    };

    mta.iter()
        .filter(|mta| mta.railroad.as_str() == railroad_str)
        .map(|mta| {
            let mut supporting_gtfs: Option<gtfs_realtime::FeedEntity> = None;

            let candidates_for_id: Vec<gtfs_realtime::FeedEntity> = input_gtfs_trips
                .entity
                .clone()
                .into_iter()
                .filter(|mta_entity| {
                    let status: bool = match mta.railroad.as_str() {
                        "MNR" => mta.train_num == mta_entity.id,
                        "LIRR" => mta.train_num == get_lirr_train_id(mta_entity),
                        _ => false,
                    };

                    status
                })
                .collect::<Vec<gtfs_realtime::FeedEntity>>();

            if !candidates_for_id.is_empty() {
                supporting_gtfs = Some(candidates_for_id[0].clone());
            }

            if mta.railroad == "LIRR" {
                //filter for vehicle only
                let candidates_for_id = candidates_for_id
                    .into_iter()
                    .filter(|mta_entity| mta_entity.vehicle.is_some())
                    .collect::<Vec<gtfs_realtime::FeedEntity>>();

                if !candidates_for_id.is_empty() {
                    supporting_gtfs = Some(candidates_for_id[0].clone());
                }
            }

            (mta, supporting_gtfs)
        })
        .map(|(mta, supporting_gtfs)| gtfs_realtime::FeedEntity {
            id: mta.train_id.clone(),
            is_deleted: None,
            trip_update: None,
            alert: None,
            shape: None,
            stop: None,
            trip_modifications: None,
            vehicle: Some(gtfs_realtime::VehiclePosition {
                vehicle: match &supporting_gtfs {
                    Some(supporting_gtfs) => {
                        supporting_gtfs.clone().vehicle.unwrap().vehicle.clone()
                    }
                    None => None,
                },
                trip: match mta.railroad.as_str() {
                    "MNR" => {
                        match &supporting_gtfs {
                            Some(supporting_gtfs) => {
                                let mut trip =
                                    supporting_gtfs.vehicle.as_ref().unwrap().trip.clone();

                                //insert route id

                                //trip.route_id = supporting_gtfs.clone().trip_update.unwrap().trip.route_id.clone();

                                if trip.is_some() {
                                    match supporting_gtfs.trip_update.is_some() {
                                        true => {
                                            trip.as_mut().unwrap().route_id.clone_from(
                                                &supporting_gtfs
                                                    .trip_update
                                                    .as_ref()
                                                    .unwrap()
                                                    .trip
                                                    .route_id,
                                            );

                                            trip.as_mut().unwrap().trip_id.clone_from(
                                                &supporting_gtfs
                                                    .trip_update
                                                    .as_ref()
                                                    .unwrap()
                                                    .trip
                                                    .trip_id,
                                            )
                                        }
                                        false => {
                                            trip.as_mut().unwrap().route_id = None;
                                        }
                                    };
                                }

                                trip
                            }
                            None => None,
                        }
                    }
                    "LIRR" => {
                        // println!("supporting gtfs {:?}", &supporting_gtfs);

                        match &supporting_gtfs {
                            Some(supporting_gtfs) => {
                                supporting_gtfs.clone().vehicle.unwrap().trip.clone()
                            }
                            None => None,
                        }
                    }
                    _ => panic!("Not MNR or LIRR"),
                },
                position: Some(gtfs_realtime::Position {
                    latitude: mta.location.latitude,
                    longitude: mta.location.longitude,
                    bearing: mta.location.heading,
                    odometer: None,
                    speed: Some(mta.location.speed.unwrap_or(0.0) * 0.44704),
                }),
                current_stop_sequence: match &supporting_gtfs {
                    Some(supporting_gtfs) => {
                        supporting_gtfs
                            .clone()
                            .vehicle
                            .unwrap()
                            .current_stop_sequence
                    }
                    None => None,
                },
                stop_id: match &supporting_gtfs {
                    Some(supporting_gtfs) => {
                        supporting_gtfs.clone().vehicle.unwrap().stop_id.clone()
                    }
                    None => None,
                },
                current_status: match &supporting_gtfs {
                    Some(supporting_gtfs) => {
                        supporting_gtfs.clone().vehicle.unwrap().current_status
                    }
                    None => None,
                },
                timestamp: Some(mta.location.timestamp as u64),
                congestion_level: None,
                occupancy_status: None,
                multi_carriage_details: vec![],
                occupancy_percentage: None,
            }),
        })
        .collect::<Vec<gtfs_realtime::FeedEntity>>()
}

pub enum MtaRailroad {
    LIRR,
    MNR,
}

pub async fn send_mta_rail_to_aspen(
    etcd: &mut etcd_client::KvClient,
    railroad: MtaRailroad,
    vehicle_position: Vec<u8>,
    trip_updates: Vec<u8>,
    feed_id: &str,
) {
    let fetch_assigned_node_meta =
        catenary::get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await;

        if let Err(e) = aspen_client {
            eprintln!("Failed to connect to Aspen at {}: {}", data.socket, e);
            return;
        }
        let aspen_client = aspen_client.unwrap();

        let tarpc_send_to_aspen = aspen_client
            .from_alpenrose(
                tarpc::context::current(),
                data.chateau_id.clone(),
                String::from(feed_id),
                Some(vehicle_position),
                Some(trip_updates),
                None,
                true,
                true,
                false,
                Some(200),
                Some(200),
                None,
                catenary::duration_since_unix_epoch().as_millis() as u64,
            )
            .await;

        match tarpc_send_to_aspen {
            Ok(_) => {
                println!(
                    "Successfully sent MTA commuter rail data sent to {}",
                    feed_id
                );
            }
            Err(e) => {
                eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::custom_rt_feeds::mta::MNR_TRIPS_FEED;

    #[tokio::test]
    async fn test_mnr() {
        let gtfs = gtfs_structures::Gtfs::from_url_async(
            "https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip",
        )
        .await
        .unwrap();

        let client = reqwest::Client::new();

        let download_gtfs_rt = get_mta_trips(&client, MNR_TRIPS_FEED).await.unwrap();

        let a = mnr_trip_id_fixer(&gtfs, download_gtfs_rt);

        for entity in a.entity {
            if let Some(trip) = entity.trip_update {
                println!("{:#?}", trip.trip.trip_id);
            }
        }
    }

    #[tokio::test]
    async fn test_lirr() {
        let client = reqwest::Client::new();

        let gtfs_rt_trips = get_mta_trips(&client, LIRR_TRIPS_FEED).await.unwrap();

        for entity in gtfs_rt_trips.entity.iter() {
            println!("{:#?}", entity.vehicle);
        }
    }
}

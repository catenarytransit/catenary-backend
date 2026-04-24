use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Amtrak GTFS stop ID -> MTA stop code used by the unified API
const AMTRAK_TO_MTA: &[(&str, &str)] = &[
    ("STS", "2SS"), // New Haven State Street
    ("NHV", "2NH"), // New Haven
    ("BRP", "2BP"), // Bridgeport
    ("STM", "2SM"), // Stamford
    ("NYP", "NYK"), // NY Moynihan Train Hall at Penn
    ("NRO", "2NR"), // New Rochelle
    ("YNY", "0YK"), // Yonkers
];

use catenary::agency_specific_types::mta_rail::MtaTrain;
use catenary::consist_v1::{
    Amenity, AmenityStatus, AmenityType, ConsistGroup, FormationStatus, Orientation,
    PassengerClass, SiriOccupancy, UnifiedConsist, VehicleElement,
};
use ecow::EcoString;

pub fn map_mta_rail_consist(train: &MtaTrain) -> UnifiedConsist {
    let mut vehicles = Vec::new();

    for (idx, car) in train.consist.cars.iter().enumerate() {
        let occupancy = match car.loading.as_str() {
            "EMPTY" => SiriOccupancy::Empty,
            "LOW" => SiriOccupancy::Low,
            "MEDIUM" => SiriOccupancy::Medium,
            "HIGH" => SiriOccupancy::High,
            _ => SiriOccupancy::Unknown,
        };

        let mut facilities = Vec::new();
        if let Some(bikes) = car.bikes {
            if bikes > 0 {
                facilities.push(Amenity {
                    amenity_type: AmenityType::BikeSpace,
                    status: AmenityStatus::Available,
                    count: Some(bikes as u16),
                });
            }
        }
        if let Some(restroom) = car.restroom {
            if restroom {
                facilities.push(Amenity {
                    amenity_type: AmenityType::Toilet,
                    status: AmenityStatus::Available,
                    count: None,
                });
            }
        }

        vehicles.push(VehicleElement {
            uic_number: EcoString::from(
                car.number
                    .map(|n: i32| n.to_string())
                    .unwrap_or_default()
                    .as_str(),
            ),
            label: car.number.map(|n: i32| EcoString::from(n.to_string())),
            order: idx as u8,
            position_on_platform: None,
            facilities,
            occupancy: Some(occupancy),
            passenger_count: car.passengers,
            passenger_class: Some(PassengerClass::Unknown),
            is_locomotive: Some(car.locomotive),
            is_revenue: car.revenue,
        });
    }

    let group = ConsistGroup {
        group_name: Some(EcoString::from(train.train_id.as_str())),
        destination: Some(EcoString::from(train.details.headsign.as_str())),
        vehicles,
        group_orientation: Some(Orientation::Unknown),
    };

    UnifiedConsist {
        global_journey_id: EcoString::from(train.train_id.as_str()),
        groups: vec![group],
        formation_status: FormationStatus::MatchesSchedule,
    }
}

#[derive(Clone, Debug)]
pub struct LirrMnrTrackData {
    /// train_num (trip short name) -> gtfs stop_id -> track string
    pub track_lookup: HashMap<String, HashMap<String, String>>,
    /// train_num -> consist 
    pub consist_lookup: HashMap<String, UnifiedConsist>,
}

pub async fn fetch_lirr_mnr_track_data(
    chateau_id: &str,
    pool: &CatenaryPostgresPool,
) -> Option<LirrMnrTrackData> {
    let is_amtrak = chateau_id == "amtrak";
    let is_mnr = chateau_id == "metro~northrailroad";
    let is_lirr = chateau_id == "longislandrailroad";

    if !is_amtrak && !is_mnr && !is_lirr {
        return None;
    }

    let mut conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error getting DB connection for {}: {}", chateau_id, e);
            return None;
        }
    };

    use catenary::schema::gtfs::stops::dsl::{
        chateau as chateau_col, code as code_col, gtfs_id, stops,
    };

    let mut code_to_ids: HashMap<String, Vec<String>> = HashMap::new();

    if is_amtrak {
        for &(amtrak_gtfs, mta_code) in AMTRAK_TO_MTA {
            code_to_ids
                .entry(mta_code.to_string())
                .or_default()
                .push(amtrak_gtfs.to_string());
        }
    } else {
        let db_chateau = chateau_id;
        let stops_list: Vec<(String, Option<String>)> = match stops
            .filter(chateau_col.eq(db_chateau))
            .filter(code_col.is_not_null())
            .select((gtfs_id, code_col))
            .load::<(String, Option<String>)>(&mut conn)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error fetching stops for {}: {}", chateau_id, e);
                return None;
            }
        };

        for (id, code) in stops_list {
            if let Some(c) = code {
                code_to_ids.entry(c).or_default().push(id);
            }
        }
    }

    if code_to_ids.is_empty() {
        return None;
    }

    let direct_client = reqwest::Client::new();
    let resp = match direct_client
        .get("https://backend-unified.mylirr.org/locations?geometry=TRACK_TURF&railroad=BOTH")
        .header("accept-version", "3.0")
        .header("cache-control", "no-cache")
        .header("dnt", "1")
        .header("origin", "https://radar.mta.info")
        .header("pragma", "no-cache")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error fetching unified locations: {}", e);
            return None;
        }
    };

    let trains: Vec<MtaTrain> = match resp.json::<Vec<MtaTrain>>().await {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error parsing unified locations response: {}", e);
            return None;
        }
    };

    let mut track_lookup: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut consist_lookup: HashMap<String, UnifiedConsist> = HashMap::new();

    for train in &trains {
        let branch = train.details.branch.as_deref().unwrap_or("");

        let dominated_by_chateau = if is_amtrak {
            branch == "Amtrak"
        } else if is_mnr {
            train.railroad == "MNR" && branch != "Amtrak"
        } else {
            train.railroad == "LIRR" && branch != "Amtrak"
        };

        if !dominated_by_chateau {
            continue;
        }

        for stop in &train.details.stops {
            let track = stop
                .sign_track
                .as_deref()
                .filter(|t| !t.is_empty())
                .or_else(|| {
                    let t = stop.t2s_track.as_str();
                    if t.is_empty() { None } else { Some(t) }
                });

            let track = match track {
                Some(t) => t,
                None => continue,
            };

            if let Some(gtfs_ids) = code_to_ids.get(&stop.code) {
                let train_entry = track_lookup.entry(train.train_num.clone()).or_default();
                for gid in gtfs_ids {
                    train_entry
                        .entry(gid.clone())
                        .or_insert_with(|| track.to_string());
                }
            }
        }
        
        let consist = map_mta_rail_consist(train);
        consist_lookup.insert(train.train_num.clone(), consist);
    }

    let gtfs_rt_url = if is_mnr {
        Some("https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr")
    } else if is_lirr {
        Some("https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr")
    } else {
        None
    };

    if let Some(url) = gtfs_rt_url {
        if let Ok(resp) = direct_client.get(url).send().await {
            if let Ok(bytes) = resp.bytes().await {
                use prost::Message;
                if let Ok(feed) = catenary::mta_gtfs_rt::mtarr::FeedMessage::decode(bytes.as_ref())
                {
                    for entity in feed.entity {
                        if let Some(trip_update) = entity.trip_update {
                            let trip_id = trip_update.trip.trip_id.unwrap_or_default();
                            let train_num_split = trip_id
                                .split('_')
                                .map(|x| x.to_string())
                                .collect::<Vec<String>>();

                            let train_num = match is_mnr {
                                true => entity.id.clone(),
                                false => match &train_num_split.get(2) {
                                    Some(x) => x.to_string(),
                                    None => trip_id.to_string(),
                                }
                                .to_string(),
                            };

                            for stu in trip_update.stop_time_update {
                                if let Some(mta_info) = stu.mta_railroad_stop_time_update {
                                    if let Some(ref track) = mta_info.track {
                                        if !track.is_empty() {
                                            if let Some(stop_id) = &stu.stop_id {
                                                // check if we want to populate it via unified codes -> gtfs stop ids. In GTFS-RT, the `stop_id` is the raw GTFS `stop_id`.
                                                // So we don't need `code_to_ids`, the feed maps directly to `gtfs_id`!
                                                let train_entry = track_lookup
                                                    .entry(train_num.to_string())
                                                    .or_default();
                                                train_entry
                                                    .entry(stop_id.to_string())
                                                    .or_insert_with(|| track.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    eprintln!("Failed to decode MTARR GTFS-RT feed");
                }
            }
        }
    }

    Some(LirrMnrTrackData { track_lookup, consist_lookup })
}

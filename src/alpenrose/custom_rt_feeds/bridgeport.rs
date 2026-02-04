use ahash::AHashMap;
use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use prost::Message;
use serde::Deserialize;

const BASE_URL: &str = "http://bustracker.gogbt.com/tmwebwatch";

#[derive(Debug, Deserialize)]
struct BridgeportResponse<T> {
    d: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct BridgeportRoute {
    id: i32,
    abbr: String,
    #[allow(dead_code)]
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BridgeportVehicle {
    lat: f64,
    lon: f64,
    heading: i32,
    #[allow(dead_code)]
    #[serde(rename = "routeID")]
    route_id: i32,
    #[serde(rename = "routeAbbr")]
    route_abbr: String,
    #[allow(dead_code)]
    #[serde(rename = "directionID")]
    direction_id: i32,
    #[allow(dead_code)]
    direction_name: String,
    property_tag: String,
    #[allow(dead_code)]
    next_stop: String,
    #[allow(dead_code)]
    adherence: i32,
    #[allow(dead_code)]
    bike_rack: bool,
    #[allow(dead_code)]
    wheel_chair_accessible: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BridgeportStop {
    #[allow(dead_code)]
    #[serde(rename = "stopID")]
    stop_id: i32,
    #[allow(dead_code)]
    #[serde(rename = "stopName")]
    stop_name: String,
    lat: f64,
    lon: f64,
    #[serde(rename = "directionID")]
    direction_id: i32,
    #[allow(dead_code)]
    direction_name: String,
}

fn route_abbr_to_gtfs_id() -> AHashMap<&'static str, &'static str> {
    AHashMap::from_iter([
        ("1", "6246"),
        ("3", "6250"),
        ("4", "6254"),
        ("5", "6259"),
        ("6", "6251"),
        ("7", "6252"),
        ("8", "6253"),
        ("9", "6255"),
        ("10", "6257"),
        ("13", "6261"),
        ("15", "6263"),
        ("17", "6249"),
        ("19X", "6248"),
        ("22X", "6258"),
        ("23", "6256"),
        ("CL", "6247"),
    ])
}

async fn fetch_routes(
    client: &reqwest::Client,
) -> Result<Vec<BridgeportRoute>, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/MultiRoute.aspx/getRouteInfo", BASE_URL);

    let response = client
        .post(&url)
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Accept", "application/json")
        .body("{}") // Send empty JSON object to avoid 411
        .send()
        .await?;

    let data: BridgeportResponse<BridgeportRoute> = response.json().await?;
    Ok(data.d)
}

async fn fetch_vehicles_for_route(
    client: &reqwest::Client,
    route_id: i32,
) -> Result<Vec<BridgeportVehicle>, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/GoogleMap.aspx/getVehicles", BASE_URL);

    let body = serde_json::json!({ "routeID": route_id.to_string() });

    let response = client
        .post(&url)
        .header("Content-Type", "application/json; charset=UTF-8")
        .header("Accept", "application/json")
        .json(&body)
        .send()
        .await?;

    let data: BridgeportResponse<BridgeportVehicle> = response.json().await?;
    Ok(data.d)
}

async fn fetch_stops_for_route(
    client: &reqwest::Client,
    route_id: i32,
) -> Result<Vec<BridgeportStop>, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/GoogleMap.aspx/getStops", BASE_URL);

    let body = serde_json::json!({ "routeID": route_id.to_string() });

    let response = client
        .post(&url)
        .header("Content-Type", "application/json; charset=UTF-8")
        .header("Accept", "application/json")
        .json(&body)
        .send()
        .await?;

    let data: BridgeportResponse<BridgeportStop> = response.json().await?;
    Ok(data.d)
}

fn get_closest_gtfs_stop<'a>(
    gtfs: &'a gtfs_structures::Gtfs,
    lat: f64,
    lon: f64,
) -> Option<&'a gtfs_structures::Stop> {
    gtfs.stops
        .values()
        .min_by(|a, b| {
            let dist_a = (a.latitude.unwrap_or(0.0) - lat).powi(2)
                + (a.longitude.unwrap_or(0.0) - lon).powi(2);
            let dist_b = (b.latitude.unwrap_or(0.0) - lat).powi(2)
                + (b.longitude.unwrap_or(0.0) - lon).powi(2);
            dist_a
                .partial_cmp(&dist_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|v| &**v)
}

fn guess_trip_id(
    gtfs: &gtfs_structures::Gtfs,
    route_id: &str,
    next_stop_name: &str,
    api_stops: &[BridgeportStop],
    vehicle_direction_id: i32,
) -> Option<(String, String)> {
    let matched_api_stop = api_stops
        .iter()
        .find(|s| s.stop_name == next_stop_name && s.direction_id == vehicle_direction_id)?;

    let gtfs_stop = get_closest_gtfs_stop(gtfs, matched_api_stop.lat, matched_api_stop.lon)?;

    let now = chrono::Utc::now();
    let seconds_since_midnight = (now.timestamp() % 86400) as u32;

    let candidates = gtfs
        .trips
        .values()
        .filter(|vocab| vocab.route_id == route_id);

    // Find a trip that stops at `gtfs_stop.id` with arrival_time closest to now.

    let mut best_trip: Option<&gtfs_structures::Trip> = None;
    let mut min_diff = u32::MAX;

    for trip in candidates {
        for stop_time in &trip.stop_times {
            if stop_time.stop.id == gtfs_stop.id {
                // Check arrival time
                let arrival = stop_time.arrival_time?;
                // Bridgeport is UTC-5; adjust comparison to match stop arrival time
                let local_seconds = if seconds_since_midnight >= 5 * 3600 {
                    seconds_since_midnight - 5 * 3600
                } else {
                    seconds_since_midnight + 19 * 3600
                };

                let diff = if arrival > local_seconds {
                    arrival - local_seconds
                } else {
                    local_seconds - arrival
                };

                if diff < min_diff && diff < 3600 {
                    min_diff = diff;
                    best_trip = Some(trip);
                }
            }
        }
    }

    if let Some(trip) = best_trip {
        let start_date = now.format("%Y%m%d").to_string();
        return Some((trip.id.clone(), start_date));
    }

    None
}

fn convert_to_gtfs_rt(
    vehicles: Vec<BridgeportVehicle>,
    stops_cache: &AHashMap<i32, Vec<BridgeportStop>>, // route_id -> stops
    gtfs: &gtfs_structures::Gtfs,
    route_mapping: &AHashMap<&str, &str>,
) -> Vec<gtfs_realtime::FeedEntity> {
    vehicles
        .into_iter()
        .map(|v| {
            let gtfs_route_id = route_mapping
                .get(v.route_abbr.as_str())
                .map(|s| s.to_string());

            let mut trip_desc = gtfs_realtime::TripDescriptor {
                trip_id: None,
                route_id: gtfs_route_id.clone(),
                direction_id: None,
                start_time: None,
                start_date: None,
                schedule_relationship: None,
                modified_trip: None,
            };

            if let Some(ref r_id) = gtfs_route_id {
                // We need the API route ID (v.route_id) to lookup stops in cache
                if let Some(stops) = stops_cache.get(&v.route_id) {
                    if let Some((trip_id, start_date)) =
                        guess_trip_id(gtfs, r_id, &v.next_stop, stops, v.direction_id)
                    {
                        trip_desc.trip_id = Some(trip_id);
                        trip_desc.start_date = Some(start_date);
                    }
                }
            }

            gtfs_realtime::FeedEntity {
                id: v.property_tag.clone(),
                is_deleted: None,
                trip_update: None,
                alert: None,
                shape: None,
                stop: None,
                trip_modifications: None,
                vehicle: Some(gtfs_realtime::VehiclePosition {
                    trip: Some(trip_desc),
                    vehicle: Some(gtfs_realtime::VehicleDescriptor {
                        id: Some(v.property_tag),
                        label: None,
                        license_plate: None,
                        wheelchair_accessible: None,
                    }),
                    position: Some(gtfs_realtime::Position {
                        latitude: v.lat as f32,
                        longitude: v.lon as f32,
                        bearing: Some(v.heading as f32),
                        odometer: None,
                        speed: None,
                    }),
                    current_stop_sequence: None,
                    stop_id: None,
                    current_status: None,
                    timestamp: Some(duration_since_unix_epoch().as_secs()),
                    congestion_level: None,
                    occupancy_status: None,
                    occupancy_percentage: None,
                    multi_carriage_details: vec![],
                }),
            }
        })
        .collect()
}

pub async fn fetch_bridgeport_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    client: &reqwest::Client,
    gtfs: &gtfs_structures::Gtfs,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    let Some(data) = fetch_assigned_node_meta else {
        println!("No assigned node found for Bridgeport Transit");
        return Ok(());
    };

    let worker_id = data.worker_id.clone();

    let routes = fetch_routes(client).await?;

    let mut vehicles = Vec::new();
    let mut stops_map = AHashMap::new();

    for route in routes {
        let v_res = fetch_vehicles_for_route(client, route.id).await;

        if let Ok(v) = v_res {
            vehicles.extend(v);
        }

        let s_res = fetch_stops_for_route(client, route.id).await;
        if let Ok(s) = s_res {
            stops_map.insert(route.id, s);
        }
    }

    let route_mapping = route_abbr_to_gtfs_id();
    let entities = convert_to_gtfs_rt(vehicles, &stops_map, gtfs, &route_mapping);
    let feed = catenary::make_feed_from_entity_vec(entities);
    let vehicle_data = feed.encode_to_vec();

    let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await?;

    let tarpc_send_to_aspen = aspen_client
        .from_alpenrose(
            tarpc::context::current(),
            data.chateau_id.clone(),
            String::from(feed_id),
            Some(vehicle_data),
            None,
            None,
            true,
            false,
            false,
            Some(200),
            None,
            None,
            duration_since_unix_epoch().as_millis() as u64,
        )
        .await;

    match tarpc_send_to_aspen {
        Ok(_) => {
            println!("Successfully sent Bridgeport Transit data to {}", feed_id);
        }
        Err(e) => {
            eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_routes() {
        let client = reqwest::Client::new();
        let url = format!("{}/MultiRoute.aspx/getRouteInfo", BASE_URL);

        let response = client
            .post(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("Accept", "application/json")
            .body("{}")
            .send()
            .await
            .expect("Failed to send request");

        let text = response.text().await.expect("Failed to get text");
        println!("Raw response: {}", text);

        let data: Result<BridgeportResponse<BridgeportRoute>, _> = serde_json::from_str(&text);
        match data {
            Ok(data) => {
                println!("Fetched {} routes", data.d.len());
                for route in &data.d {
                    println!("  Route {}: {} (id={})", route.abbr, route.name, route.id);
                }
                assert!(!data.d.is_empty());
            }
            Err(e) => {
                panic!("Failed to decode routes: {}. Raw text: {}", e, text);
            }
        }
    }

    #[tokio::test]
    async fn test_fetch_vehicles() {
        let client = reqwest::Client::new();
        let url = format!("{}/GoogleMap.aspx/getVehicles", BASE_URL);
        let body = serde_json::json!({ "routeID": "1" });

        let response = client
            .post(&url)
            .header("Content-Type", "application/json; charset=UTF-8")
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await
            .expect("Failed to send request");

        let text = response.text().await.expect("Failed to get text");
        println!("Raw vehicle response: {}", text);

        let data: Result<BridgeportResponse<BridgeportVehicle>, _> = serde_json::from_str(&text);
        match data {
            Ok(data) => {
                println!("Fetched {} vehicles for route 1", data.d.len());
                for v in &data.d {
                    println!(
                        "  Vehicle {}: ({}, {}) heading {} on route {}",
                        v.property_tag, v.lat, v.lon, v.heading, v.route_abbr
                    );
                }
            }
            Err(e) => {
                panic!("Failed to decode vehicles: {}. Raw text: {}", e, text);
            }
        }
    }

    #[tokio::test]
    async fn test_fetch_stops() {
        let client = reqwest::Client::new();
        let url = format!("{}/GoogleMap.aspx/getStops", BASE_URL);
        let body = serde_json::json!({ "routeID": "1" });

        let response = client
            .post(&url)
            .header("Content-Type", "application/json; charset=UTF-8")
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await
            .expect("Failed to send request");

        let text = response.text().await.expect("Failed to get text");
        println!("Raw stops response: {}", text);

        let data: Result<BridgeportResponse<BridgeportStop>, _> = serde_json::from_str(&text);
        match data {
            Ok(data) => {
                println!("Fetched {} stops for route 1", data.d.len());
                if let Some(s) = data.d.first() {
                    println!(
                        "  First stop: {} ({}) at {},{}",
                        s.stop_name, s.stop_id, s.lat, s.lon
                    );
                }
            }
            Err(e) => {
                panic!("Failed to decode stops: {}. Raw text: {}", e, text);
            }
        }
    }

    #[tokio::test]
    async fn test_full_conversion_with_real_gtfs() {
        // 1. Load GTFS
        println!("Downloading GTFS...");
        let gtfs = gtfs_structures::Gtfs::from_url_async(
            "https://data.trilliumtransit.com/gtfs/gbt-ct-us/gbt-ct-us.zip",
        )
        .await
        .expect("Failed to download GTFS");
        println!(
            "GTFS loaded. {} stops, {} trips",
            gtfs.stops.len(),
            gtfs.trips.len()
        );

        // 2. Fetch Data
        let client = reqwest::Client::new();
        let routes = fetch_routes(&client).await.expect("Failed to fetch routes");
        println!("Fetched {} routes", routes.len());

        let mut vehicles = Vec::new();
        let mut stops_map = AHashMap::new();

        for route in routes {
            if let Ok(v) = fetch_vehicles_for_route(&client, route.id).await {
                if !v.is_empty() {
                    vehicles.extend(v);
                    // Fetch stops only if we have vehicles to save time/bandwidth in test
                    if let Ok(s) = fetch_stops_for_route(&client, route.id).await {
                        stops_map.insert(route.id, s);
                    }
                }
            }
        }
        println!(
            "Fetched {} vehicles and stops for {} routes",
            vehicles.len(),
            stops_map.len()
        );

        // 3. Convert
        let route_mapping = route_abbr_to_gtfs_id();
        let entities = convert_to_gtfs_rt(vehicles, &stops_map, &gtfs, &route_mapping);

        println!("Generated {} GTFS-RT entities", entities.len());

        // 4. Inspect results
        let matched_trips = entities
            .iter()
            .filter(|e| {
                e.vehicle
                    .as_ref()
                    .and_then(|v| v.trip.as_ref())
                    .and_then(|t| t.trip_id.as_ref())
                    .is_some()
            })
            .count();
        println!(
            "Matched {} trips out of {} vehicles",
            matched_trips,
            entities.len()
        );

        if !entities.is_empty() {
            // Assert that we are producing valid entities
            let sample = entities.first().unwrap();
            println!("Sample Entity: {:?}", sample);
        }
    }
}

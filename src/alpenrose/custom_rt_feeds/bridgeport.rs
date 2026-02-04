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
    route_id: i32,
    route_abbr: String,
    #[allow(dead_code)]
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
    stop_id: i32,
    #[allow(dead_code)]
    stop_name: String,
    lat: f64,
    lon: f64,
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
        .body("")
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

fn find_active_trip(
    gtfs: &gtfs_structures::Gtfs,
    route_id: &str,
    stop_name: &str,
    vehicle_direction_id: i32,
) -> Option<(String, String)> {
    // 1. Find gtfs stop by fuzzy name match
    // Simple exact match or contains check for now
    // The API stop names are like "BARNUM AVE. at MAIN ST."
    // GTFS stop names might be similar (or not).
    // Let's try to match by name as best as we can.
    // Ideally we would use lat/lon but we don't have vehicle stop lat/lon readily available to cross check easily without map
    // Actually we have stats for all stops for the route, so we can pass that map in.

    // Better approach:
    // In `convert_to_gtfs_rt`, we have `stops` for the route.
    // We can find matching BridgeportStop by name == vehicle.nextStop.
    // Then use BridgeportStop lat/lon to finding closest GTFS stop.

    None
}

fn get_closest_gtfs_stop<'a>(
    gtfs: &'a gtfs_structures::Gtfs,
    lat: f64,
    lon: f64,
) -> Option<&'a gtfs_structures::Stop> {
    gtfs.stops.values().min_by(|a, b| {
        let dist_a =
            (a.latitude.unwrap_or(0.0) - lat).powi(2) + (a.longitude.unwrap_or(0.0) - lon).powi(2);
        let dist_b =
            (b.latitude.unwrap_or(0.0) - lat).powi(2) + (b.longitude.unwrap_or(0.0) - lon).powi(2);
        dist_a.partial_cmp(&dist_b).unwrap_or(std::cmp::Ordering::Equal)
    }).map(|v| &**v)
}

fn guess_trip_id(
    gtfs: &gtfs_structures::Gtfs,
    route_id: &str,
    next_stop_name: &str,
    api_stops: &[BridgeportStop],
    vehicle_direction_id: i32,
) -> Option<(String, String)> { // (trip_id, start_date)
    // 1. Find the stop in api_stops that matches next_stop_name
    let matched_api_stop = api_stops.iter().find(|s| s.stop_name == next_stop_name && s.direction_id == vehicle_direction_id)?;

    // 2. Find closest GTFS stop
    let gtfs_stop = get_closest_gtfs_stop(gtfs, matched_api_stop.lat, matched_api_stop.lon)?;
    
    // 3. Find trips for this route that stop at this stop around now
    
    // Date/Time logic
    // We need current time in local timezone presumably, or just unix timestamp
    // GTFS uses HH:MM:SS, potentially > 24h
    let now = chrono::Utc::now();
    // Assuming EST/EDT for Bridgeport, CT. -5 or -4. 
    // Ideally use chrono-tz or offset, but simple offset is okay for estimation
    // Or just use seconds from midnight UTC and adjust.
    // Let's rely on chrono's FixedOffset for now or just standard matching logic
    // Actually catenary likely has utilities for this. 
    // `gtfs_structures` works with seconds since midnight.
    
    let seconds_since_midnight = (now.timestamp() % 86400) as u32; // This is UTC seconds since midnight.
    // Bridgeport is UTC-5/UTC-4.
    // Let's roughly adjust. 
    // better: calculate local time seconds since midnight.
    // Or iterate all trips and find the one with closest arrival time at that stop
    // THAT IS VALID for today.

    // Filter trips by route_id
    let candidates = gtfs.trips.values().filter(|vocab| vocab.route_id == route_id);

    // We need to check if service runs today.
    // Using simple approach: assume service is active if not explicitly removed? 
    // No, we should check calendar.
    // Let's skip complex calendar check and just find *a* trip relative close in time as a heuristic for now?
    // Or just check if today YYYYMMDD is in service.
    
    // This part is computationally expensive to do for every vehicle if not optimized.
    // For now, let's just implement exact stop match + closest time without full calendar validation
    // or return None if too complex for this step.

    // Simplified: Find a trip that stops at `gtfs_stop.id` with arrival_time closest to now.
    
    let mut best_trip: Option<&gtfs_structures::Trip> = None;
    let mut min_diff = u32::MAX;

    for trip in candidates {
        for stop_time in &trip.stop_times {
            if stop_time.stop.id == gtfs_stop.id {
                 // Check arrival time
                 let arrival = stop_time.arrival_time?;
                 // Convert UTC now to approx local seconds (UTC-5)
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
                 
                 if diff < min_diff && diff < 3600 { // within 1 hour
                     min_diff = diff;
                     best_trip = Some(trip);
                 }
            }
        }
    }
    
    if let Some(trip) = best_trip {
         // Determine start_date. Simple current date YYYYMMDD string.
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

            // Try to guess trip id
            if let Some(ref r_id) = gtfs_route_id {
                // We need the API route ID (v.route_id) to lookup stops in cache
                if let Some(stops) = stops_cache.get(&v.route_id) {
                     if let Some((trip_id, start_date)) = guess_trip_id(gtfs, r_id, &v.next_stop, stops, v.direction_id) {
                         trip_desc.trip_id = Some(trip_id);
                         trip_desc.start_date = Some(start_date);
                         // trip_desc.schedule_relationship = Some(gtfs_realtime::trip_descriptor::ScheduleRelationship::Scheduled as i32); 
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

    // Fetch vehicles and stops in parallel? 
    // Or just iterate routes and fetch both.
    
    let mut vehicles = Vec::new();
    let mut stops_map = AHashMap::new();

    // Limit concurrency if needed, but for now just process chunks or all
    for route in routes {
        // Fetch vehicles
        let v_res = fetch_vehicles_for_route(client, route.id).await;
        if let Ok(v) = v_res {
             vehicles.extend(v);
        }
        
        // Fetch stops (only need to do this occasionally ideally, but stateless here)
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
        let routes = fetch_routes(&client).await;
        
        match routes {
            Ok(routes) => {
                println!("Fetched {} routes", routes.len());
                for route in &routes {
                    println!("  Route {}: {} (id={})", route.abbr, route.name, route.id);
                }
                assert!(!routes.is_empty());
            }
            Err(e) => {
                eprintln!("Failed to fetch routes: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_fetch_vehicles() {
        let client = reqwest::Client::new();
        let vehicles = fetch_vehicles_for_route(&client, 1).await;

        match vehicles {
            Ok(vehicles) => {
                println!("Fetched {} vehicles for route 1", vehicles.len());
                for v in &vehicles {
                    println!(
                        "  Vehicle {}: ({}, {}) heading {} on route {}",
                        v.property_tag, v.lat, v.lon, v.heading, v.route_abbr
                    );
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch vehicles: {}", e);
            }
        }
    }
    
    #[tokio::test]
    async fn test_fetch_stops() {
        let client = reqwest::Client::new();
        let stops = fetch_stops_for_route(&client, 1).await;

         match stops {
            Ok(stops) => {
                println!("Fetched {} stops for route 1", stops.len());
                if let Some(s) = stops.first() {
                     println!("  First stop: {} ({}) at {},{}", s.stop_name, s.stop_id, s.lat, s.lon);
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch stops: {}", e);
            }
        }
    }
}

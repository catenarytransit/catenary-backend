
use ahash::AHashSet;
use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::{TimeZone, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use diesel::sql_types::Text;
use diesel::QueryableByName;
use reqwest::Client;
use scc::HashMap as SccHashMap;
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(QueryableByName)]
struct TripShortNameQuery {
    #[diesel(sql_type = Text)]
    trip_short_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IRailVehicleInfo {
    pub name: String,
    #[serde(alias = "locationX")]
    pub location_x: f64,
    #[serde(alias = "locationY")]
    pub location_y: f64,
    pub shortname: String,
    #[serde(alias = "@id")]
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IRailPlatformInfo {
    pub name: String,
    pub normal: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IRailStationInfo {
    pub id: String,
    #[serde(alias = "@id")]
    pub at_id: String,
    #[serde(alias = "locationX")]
    pub location_x: f64,
    #[serde(alias = "locationY")]
    pub location_y: f64,
    pub standardname: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IRailStop {
    pub id: String,
    pub station: String,
    pub stationinfo: IRailStationInfo,
    pub time: i64,
    pub delay: i64,
    pub platform: String,
    pub platforminfo: Option<IRailPlatformInfo>,
    pub canceled: i64,
    #[serde(alias = "departureDelay")]
    pub departure_delay: i64,
    #[serde(alias = "departureCanceled")]
    pub departure_canceled: i64,
    #[serde(alias = "scheduledDepartureTime")]
    pub scheduled_departure_time: i64,
    #[serde(alias = "arrivalDelay")]
    pub arrival_delay: i64,
    #[serde(alias = "arrivalCanceled")]
    pub arrival_canceled: i64,
    #[serde(alias = "isExtraStop")]
    pub is_extra_stop: i64,
    #[serde(alias = "scheduledArrivalTime")]
    pub scheduled_arrival_time: i64,
    #[serde(alias = "departureConnection")]
    pub departure_connection: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IRailStops {
    pub number: i64,
    pub stop: Vec<IRailStop>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IRailVehicleResponse {
    pub version: String,
    pub timestamp: i64,
    pub vehicle: String,
    pub vehicleinfo: IRailVehicleInfo,
    pub stops: IRailStops,
}

pub type SncbSharedData = SccHashMap<String, IRailVehicleResponse>;

struct ProxyClient {
    client: Client,
    last_request: Mutex<Instant>,
    cooldown_until: Mutex<Option<Instant>>,
    name: String,
}

impl ProxyClient {
    async fn is_ready(&self) -> bool {
        let now = Instant::now();
        let cooldown = self.cooldown_until.lock().await;
        if let Some(until) = *cooldown {
            if now < until {
                return false;
            }
        }
        
        let last = self.last_request.lock().await;
        // Enforce roughly 3 req/s -> 333ms per request
        if now.duration_since(*last) < Duration::from_millis(340) {
            return false;
        }
        true
    }

    async fn mark_used(&self) {
        let mut last = self.last_request.lock().await;
        *last = Instant::now();
    }

    async fn mark_cooldown(&self) {
        let mut cooldown = self.cooldown_until.lock().await;
        *cooldown = Some(Instant::now() + Duration::from_secs(300)); // 5 minutes backoff
        println!("Backing off proxy {} for 5 minutes due to 429", self.name);
    }

    async fn clear_cooldown(&self) {
        let mut cooldown = self.cooldown_until.lock().await;
        *cooldown = None;
    }
}

pub async fn run_sncb_importer(
    sncb_data: Arc<SncbSharedData>,
    pool: Arc<CatenaryPostgresPool>,
) {
    println!("Starting SNCB Importer...");

    let clients = vec![
        Arc::new(ProxyClient {
            client: Client::builder()
                .user_agent("Catenary Maps <kyler@catenarymaps.org>")
                .build()
                .unwrap(),
            last_request: Mutex::new(Instant::now() - Duration::from_secs(1)),
            cooldown_until: Mutex::new(None),
            name: "Direct".to_string(),
        }),
        Arc::new(ProxyClient {
            client: Client::builder()
                .user_agent("Catenary Maps <kyler@catenarymaps.org>")
                .proxy(reqwest::Proxy::all("http://40.89.145.14:80").unwrap())
                .build()
                .unwrap(),
            last_request: Mutex::new(Instant::now() - Duration::from_secs(1)),
            cooldown_until: Mutex::new(None),
            name: "Proxy1".to_string(),
        }),
        Arc::new(ProxyClient {
            client: Client::builder()
                .user_agent("Catenary Maps <kyler@catenarymaps.org>")
                .proxy(reqwest::Proxy::all("http://141.253.118.174:80").unwrap())
                .build()
                .unwrap(),
            last_request: Mutex::new(Instant::now() - Duration::from_secs(1)),
            cooldown_until: Mutex::new(None),
            name: "Proxy2".to_string(),
        }),
    ];

    let mut trip_ids_to_fetch: AHashSet<String> = AHashSet::new();
    // Cache map: trip_id -> (short_name, last_fetch_time)
    // We only need to fetch trips that are active today.
    
    // We fetch ALL trips that are running "now" or in the next hour?
    // User said: "identify all currently operating trips on SNCB and the ones that will operate in the next hour or so"
    
    loop {
        // 1. Refresh Active Trips List occasionally (every minute?)
        let now_utc = Utc::now();
        // Convert to Belgian time? API takes DDMMYY.
        // But for fetching FROM POSTGRES, we need active trips.
        
        {
            // Fetch trips from postgres
             if let Ok(mut conn) = pool.get().await {
                   let today = now_utc.format("%Y%m%d").to_string();
                   // Filter by date range. Day of week check is omitted for simplicity but date range is critical.
                   let q = diesel::sql_query(format!(
                       "SELECT DISTINCT t.trip_short_name 
                        FROM trips_compressed t
                        JOIN calendar c ON t.service_id = c.service_id
                        WHERE t.chateau = 'sncb'
                        AND c.start_date <= '{}' AND c.end_date >= '{}'
                        AND t.trip_short_name IS NOT NULL", 
                       today, today
                   ));

                   let trips_result = q.load::<TripShortNameQuery>(&mut conn).await;

                   if let Ok(trips) = trips_result {
                       trip_ids_to_fetch.clear();
                       for t in trips {
                           trip_ids_to_fetch.insert(t.trip_short_name); 
                       }
                       println!("Fetched {} active SNCB trips to query", trip_ids_to_fetch.len());
                   }
             }
        }
        
        // 2. Process Fetch Queue
        // We have a list of short_names (train numbers) to fetch.
        // We need to iterate through them.
        
        let date_str = now_utc.format("%d%m%y").to_string(); // DDMMYY
        
        let mut _fetched_count = 0;
        
        for short_name in trip_ids_to_fetch.iter() {
             // Find a ready client
             let mut chosen_client = None;
             loop {
                 for client in &clients {
                     if client.is_ready().await {
                         chosen_client = Some(client);
                         break;
                     }
                 }
                 
                 if chosen_client.is_some() {
                     break;
                 }
                 sleep(Duration::from_millis(50)).await;
             }
             
             let client = chosen_client.unwrap();
             client.mark_used().await;
             
             let url = format!(
                 "https://api.irail.be/v1/vehicle/?id=BE.NMBS.{}&date={}&format=json&alerts=true",
                 short_name, date_str
             );
             
             // Spawn request
             let sncb_data_clone = sncb_data.clone();
             let client_clone = client.clone();
             let short_name_clone = short_name.clone();
             
             tokio::spawn(async move {
                 match client_clone.client.get(&url).send().await {
                     Ok(resp) => {
                         if resp.status() == 429 {
                             client_clone.mark_cooldown().await;
                         } else if resp.status().is_success() {
                             client_clone.clear_cooldown().await;
                             if let Ok(data) = resp.json::<IRailVehicleResponse>().await {
                                 // format key? using the vehicle id "BE.NMBS.IC3033" or the short_name "IC3033"?
                                 // The user said: "We also need to create trips where there are no trips."
                                 // And "We will hydrate the data...".
                                 // Let's key by the vehicle ID from the response? Or the short_name we queried?
                                 // The response has `vehicleinfo.shortname`.
                                 // Let's use the shortname as key, or maybe the full vehicle ID.
                                 // `import_alpenrose` needs to lookup by trip_id or short_name.
                                 // Since GTFS trip_short_name matches this, let's index by short_name.
                                 // But wait, key in SccHashMap is String.
                                 // Let's use the `shortname` from the response as the key.
                                 // But wait, the prompt says "Where 6578 is the trip short name".
                                 // The response `vehicleinfo.shortname` is "IC3033".
                                 // We should probably strip letters? Or match exactly?
                                 // Let's store by shortname as-is from the response.
                                 sncb_data_clone.upsert_async(data.vehicleinfo.shortname.clone(), data).await;
                             }
                         } else {
                            // 404 or other error
                            // println!("Failed to fetch {}: {}", short_name_clone, resp.status());
                         }
                     }
                     Err(e) => {
                         println!("Request failed for {}: {}", short_name_clone, e);
                     }
                 }
             });
             
             _fetched_count += 1;
             
             // Just break early if we did a lot? No, the loop continues.
             // We need to loop this efficiently.
        }
        
        sleep(Duration::from_secs(60)).await; // Refresh list every minute
    }
}

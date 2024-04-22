// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

extern crate catenary;
use catenary::aspen_dataset::*;
use catenary::parse_gtfs_rt_message;
use catenary::postgres_tools::CatenaryPostgresPool;
use dashmap::DashMap;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use gtfs_rt::TripUpdate;
use prost::Message;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const MAKE_VEHICLES_FEED_LIST: [&str; 9] = [
    "f-mta~nyc~rt~subway~1~2~3~4~5~6~7",
    "f-mta~nyc~rt~subway~a~c~e",
    "f-mta~nyc~rt~subway~b~d~f~m",
    "f-mta~nyc~rt~subway~g",
    "f-mta~nyc~rt~subway~j~z",
    "f-mta~nyc~rt~subway~l",
    "f-mta~nyc~rt~subway~n~q~r~w",
    "f-mta~nyc~rt~subway~sir",
    "f-bart~rt",
];

pub async fn new_rt_data(
    authoritative_data_store: Arc<DashMap<String, RwLock<catenary::aspen_dataset::AspenisedData>>>,
    chateau_id: String,
    realtime_feed_id: String,
    vehicles: Option<Vec<u8>>,
    trips: Option<Vec<u8>>,
    alerts: Option<Vec<u8>>,
    has_vehicles: bool,
    has_trips: bool,
    has_alerts: bool,
    vehicles_response_code: Option<u16>,
    trips_response_code: Option<u16>,
    alerts_response_code: Option<u16>,
    pool: Arc<CatenaryPostgresPool>,
) -> bool {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let vehicle = match vehicles {
        Some(v) => match parse_gtfs_rt_message(&v.as_slice()) {
            Ok(v) => Some(v),
            Err(e) => {
                println!("Error decoding vehicles: {}", e);
                None
            }
        },
        None => None,
    };

    let trip = match trips {
        Some(t) => match parse_gtfs_rt_message(&t.as_slice()) {
            Ok(t) => Some(t),
            Err(e) => {
                println!("Error decoding trips: {}", e);
                None
            }
        },
        None => None,
    };

    let alert = match alerts {
        Some(a) => match parse_gtfs_rt_message(&a.as_slice()) {
            Ok(a) => Some(a),
            Err(e) => {
                println!("Error decoding alerts: {}", e);
                None
            }
        },
        None => None,
    };

    //get and update raw gtfs_rt data

    let this_chateau_dashmap = authoritative_data_store.get(&realtime_feed_id);

    //if this item is empty, create it
    if this_chateau_dashmap.is_none() {
        let mut new_aspenised_data = catenary::aspen_dataset::AspenisedData {
            vehicle_positions: Vec::new(),
            vehicle_routes_cache: HashMap::new(),
            trip_updates: HashMap::new(),
            trip_updates_lookup_by_trip_id_to_trip_update_ids: HashMap::new(),
            raw_alerts: None,
            impacted_routes_alerts: None,
            impacted_stops_alerts: None,
            impacted_routes_stops_alerts: None,
            raw_gtfs_rt: BTreeMap::new(),
        };
        authoritative_data_store.insert(realtime_feed_id.clone(), RwLock::new(new_aspenised_data));
    }

    //now it exists!
    let this_chateau_dashmap = authoritative_data_store.get(&realtime_feed_id).unwrap();

    let mut this_chateau_lock = this_chateau_dashmap.write().await;

    let mutable_raw_gtfs_rt = this_chateau_lock
        .raw_gtfs_rt
        .get_mut(&realtime_feed_id.clone());

    match mutable_raw_gtfs_rt {
        Some(m) => {
            if m.vehicle_positions.is_none() && vehicle.is_some() {
                m.vehicle_positions = vehicle;
            }

            if m.trip_updates.is_none() && trip.is_some() {
                m.trip_updates = trip;
            }

            if m.alerts.is_none() && alert.is_some() {
                m.alerts = alert;
            }
        }
        None => {
            let mut new_gtfs_rt_data = catenary::aspen_dataset::GtfsRtDataStore {
                vehicle_positions: vehicle,
                trip_updates: trip,
                alerts: alert,
            };
            this_chateau_lock
                .raw_gtfs_rt
                .insert(realtime_feed_id.clone(), new_gtfs_rt_data);
        }
    }

    // take all the gtfs rt data and merge it together

    let mut vehicle_positions: Vec<AspenisedVehiclePosition> = Vec::new();
    let mut vehicle_routes_cache: HashMap<String, AspenisedVehicleRouteCache> = HashMap::new();
    let mut trip_updates: HashMap<String, TripUpdate> = HashMap::new();
    let mut trip_updates_lookup_by_trip_id_to_trip_update_ids: HashMap<String, Vec<String>> =
        HashMap::new();

    use catenary::schema::gtfs::routes as routes_pg_schema;

    //get all routes inside chateau from postgres db
    let routes = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select((catenary::models::Route::as_select()))
        .load::<catenary::models::Route>(conn)
        .await
        .unwrap();

    for (realtime_feed_id, gtfs_dataset) in this_chateau_lock.raw_gtfs_rt.iter() {
        if gtfs_dataset.vehicle_positions.is_some() {

            //for trips, batch lookups by groups of 100
            //collect all common itinerary patterns and look those up

            //combine them together and insert them with the vehicles positions
        }

        // trips can be left fairly raw for now, with a lot of data references

        // ignore alerts for now, as well as trip modifications
    }

    true
}

use catenary::aspen::lib::AlertsforManyStops;

use catenary::aspen_dataset::AspenisedAlert;
use scc::HashMap as SccHashMap;
use std::sync::Arc;

pub fn get_alerts_from_route_id(
    authoritative_data_store: Arc<SccHashMap<String, Arc<catenary::aspen_dataset::AspenisedData>>>,
    chateau_id: &str,
    route_id: &str,
) -> Option<Vec<(String, AspenisedAlert)>> {
    let snapshot = {
        let guard = authoritative_data_store.get_sync(chateau_id)?;
        Arc::clone(guard.get())
    };

    match snapshot.impacted_routes_alerts.get(route_id) {
        Some(alerts) => {
            let mut alerts_vec = Vec::new();

            for alert_id in alerts {
                let alert = snapshot.aspenised_alerts.get(alert_id);

                match alert {
                    Some(alert) => {
                        alerts_vec.push((alert_id.clone(), alert.clone()));
                    }
                    None => {
                        println!("Alert not found for alert id {}", alert_id);
                    }
                }
            }

            Some(alerts_vec)
        }
        None => {
            println!("Route id not found in alerts lookup table");
            None
        }
    }
}

pub fn get_alerts_from_stop_id(
    authoritative_data_store: Arc<SccHashMap<String, Arc<catenary::aspen_dataset::AspenisedData>>>,
    chateau_id: &str,
    stop_id: &str,
) -> Option<Vec<(String, AspenisedAlert)>> {
    let snapshot = {
        let guard = authoritative_data_store.get_sync(chateau_id)?;
        Arc::clone(guard.get())
    };

    match snapshot.impacted_stops_alerts.get(stop_id) {
        Some(alerts) => {
            let mut alerts_vec = Vec::new();

            for alert_id in alerts {
                let alert = snapshot.aspenised_alerts.get(alert_id);

                match alert {
                    Some(alert) => {
                        alerts_vec.push((alert_id.clone(), alert.clone()));
                    }
                    None => {
                        println!("Alert not found for alert id {}", alert_id);
                    }
                }
            }

            Some(alerts_vec)
        }
        None => {
            println!("Stop id not found in alerts lookup table");
            None
        }
    }
}

pub fn get_alert_from_trip_id(
    authoritative_data_store: Arc<SccHashMap<String, Arc<catenary::aspen_dataset::AspenisedData>>>,
    chateau_id: &str,
    trip_id: &str,
) -> Option<Vec<(String, AspenisedAlert)>> {
    let snapshot = {
        let guard = authoritative_data_store.get_sync(chateau_id)?;
        Arc::clone(guard.get())
    };

    match snapshot.impacted_trips_alerts.get(trip_id) {
        Some(alerts) => {
            let mut alerts_vec = Vec::new();

            for alert_id in alerts {
                let alert = snapshot.aspenised_alerts.get(alert_id);

                match alert {
                    Some(alert) => {
                        alerts_vec.push((alert_id.clone(), alert.clone()));
                    }
                    None => {
                        println!("Alert not found for alert id {}", alert_id);
                    }
                }
            }

            Some(alerts_vec)
        }
        None => {
            println!("Trip id not found in alerts lookup table");
            None
        }
    }
}

pub fn get_alert_from_stop_ids(
    authoritative_data_store: Arc<SccHashMap<String, Arc<catenary::aspen_dataset::AspenisedData>>>,
    chateau_id: &str,
    stop_ids: Vec<String>,
) -> Option<AlertsforManyStops> {
    None
}

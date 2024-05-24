use catenary::aspen_dataset::AspenisedAlert;
use std::sync::Arc;
use scc::HashMap as SccHashMap;
use std::collections::HashMap;

pub fn get_alerts_from_route_id(
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    chateau_id: &str,
    route_id: &str,
) -> Option<Vec<AspenisedAlert>> {
    match authoritative_data_store.get(chateau_id) {
        Some(aspenised_data) => {
            let aspenised_data = aspenised_data.get();

            let alerts = aspenised_data.impacted_routes_alerts.get(route_id);

            match alerts {
                Some(alerts) => {
                    let mut alerts_vec = Vec::new();

                    for alert_id in alerts {
                        let alert = aspenised_data.aspenised_alerts.get(alert_id);

                        match alert {
                            Some(alert) => {
                                alerts_vec.push(alert.clone());
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
        None => None,
    }
}

pub fn get_alerts_from_stop_id(
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    chateau_id: &str,
    stop_id: &str,
) -> Option<Vec<AspenisedAlert>> {
    match authoritative_data_store.get(chateau_id) {
        Some(aspenised_data) => {
            let aspenised_data = aspenised_data.get();

            let alerts = aspenised_data.impacted_stops_alerts.get(stop_id);

            match alerts {
                Some(alerts) => {
                    let mut alerts_vec = Vec::new();

                    for alert_id in alerts {
                        let alert = aspenised_data.aspenised_alerts.get(alert_id);

                        match alert {
                            Some(alert) => {
                                alerts_vec.push(alert.clone());
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
        None => None,
    }
}

pub fn get_alert_from_stop_ids(
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    chateau_id: &str,
    stop_ids: Vec<String>,
) -> Option<HashMap<String, Vec<AspenisedAlert>>> {
    match authoritative_data_store.get(chateau_id) {
        Some(aspenised_data) => {
            let aspenised_data = aspenised_data.get();

            let mut alerts_map = HashMap::new();

            for stop_id in stop_ids {
                let alerts = aspenised_data.impacted_stops_alerts.get(&stop_id);

                match alerts {
                    Some(alerts) => {
                        let mut alerts_vec = Vec::new();

                        for alert_id in alerts {
                            let alert = aspenised_data.aspenised_alerts.get(alert_id);

                            match alert {
                                Some(alert) => {
                                    alerts_vec.push(alert.clone());
                                }
                                None => {
                                    println!("Alert not found for alert id {}", alert_id);
                                }
                            }
                        }

                        alerts_map.insert(stop_id, alerts_vec);
                    }
                    None => {
                        println!("Stop id not found in alerts lookup table");
                    }
                }
            }

            Some(alerts_map)
        }
        None => None,
    }
}

pub fn get_alert_from_trip_id(
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    chateau_id: &str,
    trip_id: &str,
) -> Option<Vec<AspenisedAlert>> {
    match authoritative_data_store.get(chateau_id) {
        Some(aspenised_data) => {
            let aspenised_data = aspenised_data.get();

            let alerts = aspenised_data.impacted_trips_alerts.get(trip_id);

            match alerts {
                Some(alerts) => {
                    let mut alerts_vec = Vec::new();

                    for alert_id in alerts {
                        let alert = aspenised_data.aspenised_alerts.get(alert_id);

                        match alert {
                            Some(alert) => {
                                alerts_vec.push(alert.clone());
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
        None => None,
    }
}
use catenary::models::Route;
use catenary::models::Stop;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct Transfers {
    pub transfers: HashMap<String, HashMap<String, Vec<IndividualTransferInfo>>>,
    pub routes: HashMap<String, HashMap<String, Route>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndividualTransferInfo {
    pub route_id: String,
    pub chateau: String,
    pub stop_id: String,
    pub distance_metres: f32,
}

pub async fn transfer_calc() -> () {}

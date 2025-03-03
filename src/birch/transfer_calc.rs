use catenary::models::Route;
use catenary::models::Stop;
use catenary::postgres_tools::CatenaryPostgresPool;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

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

pub async fn transfer_calc(
    pool: Arc<CatenaryPostgresPool>,
    input_stops: Vec<&Stop>,
    radius_metres: f32,
) -> () {
}

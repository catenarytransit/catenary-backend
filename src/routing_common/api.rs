use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRequest {
    pub start_lat: f64,
    pub start_lon: f64,
    pub end_lat: f64,
    pub end_lon: f64,
    pub time: u64,               // Unix timestamp
    pub is_departure_time: bool, // true = depart at, false = arrive by
    pub mode: TravelMode,
    pub speed_mps: f64,
    pub wheelchair_accessible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TravelMode {
    Walk,
    Bike,
    Transit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingResult {
    pub itineraries: Vec<Itinerary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Itinerary {
    pub start_time: u64,
    pub end_time: u64,
    pub duration_seconds: u64,
    pub transfers: u32,
    pub reliability_score: f64,
    pub legs: Vec<Leg>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Leg {
    pub mode: TravelMode,
    pub start_stop_id: Option<String>,
    pub end_stop_id: Option<String>,
    pub route_id: Option<String>,
    pub trip_id: Option<String>,
    pub start_stop_name: Option<String>,
    pub end_stop_name: Option<String>,
    pub route_name: Option<String>,
    pub trip_name: Option<String>,
    pub duration_seconds: u64,
    pub geometry: Vec<(f64, f64)>,
}

#[tarpc::service]
pub trait EdelweissService {
    async fn route(req: RoutingRequest) -> RoutingResult;
}

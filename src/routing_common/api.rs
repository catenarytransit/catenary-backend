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
#[serde(tag = "type")]
pub enum Leg {
    Osm(OsmLeg),
    Transit(TransitLeg),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OsmLeg {
    pub start_time: u64,
    pub end_time: u64,
    pub mode: TravelMode,
    pub start_stop_id: Option<String>,
    pub end_stop_id: Option<String>,
    pub start_stop_chateau: Option<String>,
    pub end_stop_chateau: Option<String>,
    pub start_stop_name: Option<String>,
    pub end_stop_name: Option<String>,
    pub duration_seconds: u64,
    pub geometry: Vec<(f64, f64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitLeg {
    pub start_time: u64,
    pub end_time: u64,
    pub mode: TravelMode,
    pub start_stop_id: String,
    pub end_stop_id: String,
    pub start_stop_chateau: String,
    pub end_stop_chateau: String,
    pub route_id: String,
    pub trip_id: Option<String>,
    pub chateau: String,
    pub start_stop_name: Option<String>,
    pub end_stop_name: Option<String>,
    pub route_name: Option<String>,
    pub trip_name: Option<String>,
    pub duration_seconds: u64,
    pub geometry: Vec<(f64, f64)>,
}

impl Leg {
    pub fn mode(&self) -> TravelMode {
        match self {
            Leg::Osm(l) => l.mode.clone(),
            Leg::Transit(l) => l.mode.clone(),
        }
    }

    pub fn start_time(&self) -> u64 {
        match self {
            Leg::Osm(l) => l.start_time,
            Leg::Transit(l) => l.start_time,
        }
    }

    pub fn end_time(&self) -> u64 {
        match self {
            Leg::Osm(l) => l.end_time,
            Leg::Transit(l) => l.end_time,
        }
    }

    pub fn start_stop_id(&self) -> Option<&String> {
        match self {
            Leg::Osm(l) => l.start_stop_id.as_ref(),
            Leg::Transit(l) => Some(&l.start_stop_id),
        }
    }

    pub fn end_stop_id(&self) -> Option<&String> {
        match self {
            Leg::Osm(l) => l.end_stop_id.as_ref(),
            Leg::Transit(l) => Some(&l.end_stop_id),
        }
    }

    pub fn start_stop_chateau(&self) -> Option<&String> {
        match self {
            Leg::Osm(l) => l.start_stop_chateau.as_ref(),
            Leg::Transit(l) => Some(&l.start_stop_chateau),
        }
    }

    pub fn end_stop_chateau(&self) -> Option<&String> {
        match self {
            Leg::Osm(l) => l.end_stop_chateau.as_ref(),
            Leg::Transit(l) => Some(&l.end_stop_chateau),
        }
    }

    pub fn route_id(&self) -> Option<&String> {
        match self {
            Leg::Osm(_) => None,
            Leg::Transit(l) => Some(&l.route_id),
        }
    }

    pub fn chateau(&self) -> Option<&String> {
        match self {
            Leg::Osm(_) => None,
            Leg::Transit(l) => Some(&l.chateau),
        }
    }

    pub fn set_start_stop_name(&mut self, name: Option<String>) {
        match self {
            Leg::Osm(l) => l.start_stop_name = name,
            Leg::Transit(l) => l.start_stop_name = name,
        }
    }

    pub fn set_end_stop_name(&mut self, name: Option<String>) {
        match self {
            Leg::Osm(l) => l.end_stop_name = name,
            Leg::Transit(l) => l.end_stop_name = name,
        }
    }

    pub fn set_route_name(&mut self, name: Option<String>) {
        match self {
            Leg::Osm(_) => {}
            Leg::Transit(l) => l.route_name = name,
        }
    }

    pub fn route_name(&self) -> Option<&String> {
        match self {
            Leg::Osm(_) => None,
            Leg::Transit(l) => l.route_name.as_ref(),
        }
    }

    pub fn trip_name(&self) -> Option<&String> {
        match self {
            Leg::Osm(_) => None, // OsmLeg doesn't have trip_name
            Leg::Transit(l) => l.trip_name.as_ref(),
        }
    }

    pub fn start_stop_name(&self) -> Option<&String> {
        match self {
            Leg::Osm(l) => l.start_stop_name.as_ref(),
            Leg::Transit(l) => l.start_stop_name.as_ref(),
        }
    }

    pub fn end_stop_name(&self) -> Option<&String> {
        match self {
            Leg::Osm(l) => l.end_stop_name.as_ref(),
            Leg::Transit(l) => l.end_stop_name.as_ref(),
        }
    }

    pub fn duration_seconds(&self) -> u64 {
        match self {
            Leg::Osm(l) => l.duration_seconds,
            Leg::Transit(l) => l.duration_seconds,
        }
    }

    pub fn geometry(&self) -> &Vec<(f64, f64)> {
        match self {
            Leg::Osm(l) => &l.geometry,
            Leg::Transit(l) => &l.geometry,
        }
    }
}

#[tarpc::service]
pub trait EdelweissService {
    async fn route(req: RoutingRequest) -> RoutingResult;
}

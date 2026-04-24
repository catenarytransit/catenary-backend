use catenary::aspen_dataset::AspenisedData;
use scc::HashMap as SccHashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use catenary::agency_specific_types::mta_subway::{MtaSubwayTrips, Trip, Consist, ConsistCar};
use catenary::consist_v1::{
    UnifiedConsist, ConsistGroup, VehicleElement, PlatformSpatialPosition, 
    Amenity, AmenityStatus, AmenityType, FormationStatus, Orientation, SiriOccupancy, PassengerClass
};
use reqwest::Client;
use ecow::EcoString;

pub fn map_nyct_trip_to_consist(trip: &Trip) -> UnifiedConsist {
    let mut vehicles = Vec::new();
    
    // NYCT Subway uses static positions and cars usually don't have distinct amenity status.
    for (idx, car) in trip.consist_cars.iter().flatten().enumerate() {
        vehicles.push(VehicleElement {
            uic_number: EcoString::from(car.number.as_str()),
            label: Some(EcoString::from(car.number.as_str())),
            order: idx as u8,
            position_on_platform: None,
            facilities: vec![],
            occupancy: Some(SiriOccupancy::Unknown),
            passenger_count: None,
            passenger_class: Some(PassengerClass::Unknown),
            is_locomotive: Some(car.type_field.as_deref() == Some("Locomotive")),
            is_revenue: Some(true),
        });
    }

    let group = ConsistGroup {
        group_name: Some(EcoString::from(trip.trip_id.as_str())),
        destination: Some(EcoString::from(trip.headsign.as_str())),
        vehicles,
        group_orientation: Some(Orientation::Unknown),
    };

    UnifiedConsist {
        global_journey_id: EcoString::from(trip.trip_id.as_str()),
        groups: vec![group],
        formation_status: FormationStatus::MatchesSchedule, // Assuming true for NYCT
    }
}

pub async fn bg_fetch_nyct_consists(data_store: Arc<SccHashMap<String, AspenisedData>>) {
    let client = Client::new();
    
    loop {
            Ok(res) => {
                if let Ok(data) = res.json::<MtaSubwayTrips>().await {
                    if let Some(mut aspenised_data) = data_store.get_async("nyct").await {
                        let mut nyct_data = aspenised_data.get_mut();

                        for trip in data.trips {
                            let consist = map_nyct_trip_to_consist(&trip);
                            
                            // To match LIRR logic and NYCT: Connect via global_journey_id which is the train_id/trip_id.
                            // In NYCT, AspenisedTripUpdate is mapped via trip_id in `gtfs-realtime`. 
                            if let Some(mut trip_update) = nyct_data.trip_updates.get_mut(trip.trip_id.as_str()) {
                                trip_update.consist = Some(consist.clone());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch NYCT subway trips for Consist data: {}", e);
            }
        }
        
        sleep(Duration::from_secs(10)).await;
    }
}

// LIRR / MNR structures redefined from alpenrose because alpenrose is a bin, not exposed
#[derive(serde_derive::Deserialize)]
    pub number: Option<i32>,
    pub loading: String,
    pub passengers: Option<u32>,
    pub bikes: Option<i32>,
    pub locomotive: bool,
    pub revenue: Option<bool>,
}

#[derive(serde_derive::Deserialize)]
}

#[derive(serde_derive::Deserialize)]
    pub headsign: String,
}

#[derive(serde_derive::Deserialize)]
#[serde(rename_all = "camelCase")]
    pub train_id: String,
}

    let mut vehicles = Vec::new();
    
    for (idx, car) in train.consist.cars.iter().enumerate() {
        let occupancy = match car.loading.as_str() {
            "EMPTY" => SiriOccupancy::Empty,
            "LOW" => SiriOccupancy::Low,
            "MEDIUM" => SiriOccupancy::Medium,
            "HIGH" => SiriOccupancy::High,
            _ => SiriOccupancy::Unknown,
        };

        let mut facilities = Vec::new();
        if let Some(bikes) = car.bikes {
            if bikes > 0 {
                facilities.push(Amenity {
                    amenity_type: AmenityType::BikeSpace,
                    status: AmenityStatus::Available,
                    count: Some(bikes as u16),
                });
            }
        }
        
        vehicles.push(VehicleElement {
            uic_number: EcoString::from(car.number.map(|n: i32| n.to_string()).unwrap_or_default().as_str()),
            label: car.number.map(|n: i32| EcoString::from(n.to_string())),
            order: idx as u8,
            position_on_platform: None, 
            facilities,
            occupancy: Some(occupancy),
            passenger_count: car.passengers,
            passenger_class: Some(PassengerClass::Unknown),
            is_locomotive: Some(car.locomotive),
            is_revenue: car.revenue,
        });
    }

    let group = ConsistGroup {
        group_name: Some(EcoString::from(train.train_id.as_str())),
        destination: Some(EcoString::from(train.details.headsign.as_str())),
        vehicles,
        group_orientation: Some(Orientation::Unknown),
    };

    UnifiedConsist {
        global_journey_id: EcoString::from(train.train_id.as_str()),
        groups: vec![group],
        formation_status: FormationStatus::MatchesSchedule,
    }
}

    let client = Client::new();
    
    loop {
            Ok(res) => {
                    if let Some(mut aspenised_data) = data_store.get_async("LongIslandRailRoad").await { // assuming LIRR chateau

                        for train in trains {
                            
                            // LIRR uses train_id/trip_id. Needs testing.
                                trip_update.consist = Some(consist.clone());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch LIRR generic locations for Consist data: {}", e);
            }
        }
        
        sleep(Duration::from_secs(10)).await;
    }
}

use catenary::aspen_dataset::AspenisedData;
use scc::HashMap as SccHashMap;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

use std::collections::HashMap;

use catenary::agency_specific_types::mta_rail::MtaTrain;
use catenary::agency_specific_types::mta_subway;
use catenary::agency_specific_types::mta_subway::{MtaSubwayTrips, Trip};
use catenary::consist_v1::{
    Amenity, AmenityStatus, AmenityType, ConsistGroup, FormationStatus, Orientation,
    PassengerClass, SiriOccupancy, UnifiedConsist, VehicleElement,
};
use ecow::EcoString;
use reqwest::Client;

/// Maps an NYCT Subway Trip to a UnifiedConsist structure.
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
        group_name: Some(EcoString::from(trip.nyct_train_id.as_str())),
        destination: Some(EcoString::from(trip.headsign.as_str())),
        vehicles,
        group_orientation: Some(Orientation::Unknown),
    };

    UnifiedConsist {
        global_journey_id: EcoString::from(trip.nyct_train_id.as_str()),
        groups: vec![group],
        formation_status: FormationStatus::MatchesSchedule,
    }
}

/// Background task to fetch subway consists from the Helium API.
pub async fn bg_fetch_nyct_consists(
    data_store: Arc<
        tokio::sync::RwLock<
            Option<HashMap<String, catenary::agency_specific_types::mta_subway::Trip>>,
        >,
    >,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let client = Client::new();

    loop {
        match client
            .get("https://helium-prod.mylirr.org/v1/subway/trips")
            .send()
            .await
        {
            Ok(res) => {
                if let Ok(data) = res.json::<MtaSubwayTrips>().await {
                    println!("Fetched {} subway trips for Consist data", data.trips.len());

                    let mut trip_map = HashMap::new();
                    for trip in data.trips {
                        trip_map.insert(trip.nyct_train_id.clone(), trip);
                    }

                    let mut write_guard = data_store.write().await;
                    *write_guard = Some(trip_map);
                    println!(
                        "Updated authoritative NYCT subway data cache with {} trips",
                        write_guard.as_ref().unwrap().len()
                    );
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch NYCT subway trips for Consist data: {}", e);
            }
        }

        sleep(Duration::from_secs(10)).await;
    }

    Err(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        "bg_fetch_nyct_consists task terminated unexpectedly",
    )))
}

/// Maps an MTA Rail (LIRR/MNR) Train to a UnifiedConsist structure.
pub fn map_mta_rail_train_to_consist(train: &MtaTrain) -> UnifiedConsist {
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
            uic_number: EcoString::from(car.number.map(|n| n.to_string()).unwrap_or_default()),
            label: car.number.map(|n| EcoString::from(n.to_string())),
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

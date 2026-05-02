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
                match res.json::<MtaSubwayTrips>().await {
                    Ok(data) => {
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
                    Err(e) => {
                        eprintln!("Failed to fetch NYCT subway trips for Consist data: {}", e);
                    }
                };
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

use compact_str::CompactString;

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinScheduleFormations {
    #[serde(rename = "@rid")]
    pub rid: CompactString,
    #[serde(rename = "formation", default)]
    pub formations: Vec<DarwinFormation>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinFormation {
    #[serde(rename = "@fid")]
    pub fid: CompactString,
    #[serde(rename = "coaches")]
    pub coaches: DarwinCoachList,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinCoachList {
    #[serde(rename = "coach", default)]
    pub coaches: Vec<DarwinCoachData>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinCoachData {
    #[serde(rename = "@coachNumber")]
    pub coach_number: CompactString,
    #[serde(rename = "@coachClass")]
    pub coach_class: Option<CompactString>,
    #[serde(rename = "toilet", default)]
    pub toilet: Option<DarwinToiletAvailabilityType>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinToiletAvailabilityType {
    #[serde(rename = "$value", default)]
    pub status: Option<CompactString>,
    #[serde(rename = "@status", default)]
    pub status_attr: Option<CompactString>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinScheduleFormationsV1 {
    #[serde(rename = "@rid")]
    pub rid: CompactString,
    #[serde(rename = "formation", default)]
    pub formations: Vec<DarwinFormationV1>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinFormationV1 {
    #[serde(rename = "@fid")]
    pub fid: CompactString,
    #[serde(rename = "coaches")]
    pub coaches: DarwinCoachListV1,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinCoachListV1 {
    #[serde(rename = "coach", default)]
    pub coaches: Vec<DarwinCoachDataV1>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct DarwinCoachDataV1 {
    #[serde(rename = "@coachNumber")]
    pub coach_number: CompactString,
    #[serde(rename = "@coachClass")]
    pub coach_class: Option<CompactString>,
}

/// Maps a Darwin formation to a UnifiedConsist structure.
pub fn map_darwin_formation_to_consist(
    trip_id: &str,
    schedule_formation: &DarwinScheduleFormations,
) -> UnifiedConsist {
    let mut groups = Vec::new();

    for formation in &schedule_formation.formations {
        let mut vehicles = Vec::new();
        for (idx, coach) in formation.coaches.coaches.iter().enumerate() {
            let mut facilities = Vec::new();

            if let Some(toilet) = &coach.toilet {
                let status_str = toilet
                    .status
                    .as_deref()
                    .or(toilet.status_attr.as_deref())
                    .unwrap_or("Unknown");
                let status = match status_str {
                    "InService" => AmenityStatus::Available,
                    "NotInService" => AmenityStatus::NotAvailable,
                    _ => AmenityStatus::Unknown,
                };

                facilities.push(Amenity {
                    amenity_type: AmenityType::Toilet,
                    status,
                    count: None,
                });
            }

            let pass_class = match coach.coach_class.as_deref() {
                Some("First") => PassengerClass::First,
                Some("Standard") => PassengerClass::Second,
                Some("Mixed") => PassengerClass::Unknown,
                _ => PassengerClass::Unknown,
            };

            vehicles.push(VehicleElement {
                uic_number: EcoString::from(coach.coach_number.as_str()),
                label: Some(EcoString::from(coach.coach_number.as_str())),
                order: idx as u8,
                position_on_platform: None,
                facilities,
                occupancy: None,
                passenger_count: None,
                passenger_class: Some(pass_class),
                is_locomotive: None,
                is_revenue: Some(true),
            });
        }

        groups.push(ConsistGroup {
            group_name: Some(EcoString::from(formation.fid.as_str())),
            destination: None, // Darwin payload does not directly provide segment destination at coach level
            vehicles,
            group_orientation: Some(Orientation::Unknown),
        });
    }

    UnifiedConsist {
        global_journey_id: EcoString::from(trip_id),
        groups,
        formation_status: FormationStatus::MatchesSchedule,
    }
}

/// Maps a Darwin formation v1 to a UnifiedConsist structure.
pub fn map_darwin_v1_formation_to_consist(
    trip_id: &str,
    schedule_formation: &DarwinScheduleFormationsV1,
) -> UnifiedConsist {
    let mut groups = Vec::new();

    for formation in &schedule_formation.formations {
        let mut vehicles = Vec::new();
        for (idx, coach) in formation.coaches.coaches.iter().enumerate() {
            let pass_class = match coach.coach_class.as_deref() {
                Some("First") => PassengerClass::First,
                Some("Standard") => PassengerClass::Second,
                Some("Mixed") => PassengerClass::Unknown,
                _ => PassengerClass::Unknown,
            };

            vehicles.push(VehicleElement {
                uic_number: EcoString::from(coach.coach_number.as_str()),
                label: Some(EcoString::from(coach.coach_number.as_str())),
                order: idx as u8,
                position_on_platform: None,
                facilities: vec![], // No toilet data in v1
                occupancy: None,
                passenger_count: None,
                passenger_class: Some(pass_class),
                is_locomotive: None,
                is_revenue: Some(true),
            });
        }

        groups.push(ConsistGroup {
            group_name: Some(EcoString::from(formation.fid.as_str())),
            destination: None,
            vehicles,
            group_orientation: Some(Orientation::Unknown),
        });
    }

    UnifiedConsist {
        global_journey_id: EcoString::from(trip_id),
        groups,
        formation_status: FormationStatus::MatchesSchedule,
    }
}

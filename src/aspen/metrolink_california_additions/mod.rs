use crate::import_alpenrose::MetrolinkPos;
use ahash::AHashMap;
use catenary::aspen_dataset::*;
use compact_str::CompactString;
use anyhow::Context;
use gtfs_realtime::FeedMessage;
use std::path::PathBuf;
use std::sync::OnceLock;

const DEFAULT_INVALID_POSITIONS_FILE: &str = "metrolink-invalid-positions.txt";
const METROLINK_FEED_ID: &str = "f-metrolinktrains~rt";
static INVALID_GTFS_RT_POSITIONS: OnceLock<Vec<(f32, f32)>> = OnceLock::new();

/// Read once at startup; coordinate data is never embedded in the executable.
pub fn load_invalid_gtfs_rt_positions() -> anyhow::Result<()> {
    let override_path = std::env::var_os("ASPEN_METROLINK_INVALID_POSITIONS_FILE");
    let path = override_path
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_INVALID_POSITIONS_FILE));

    let rules = match std::fs::read_to_string(&path) {
        Ok(contents) => parse_invalid_positions(&contents)
            .with_context(|| format!("Invalid Metrolink position file {}", path.display()))?,
        Err(error)
            if override_path.is_none() && error.kind() == std::io::ErrorKind::NotFound =>
        {
            tracing::warn!(
                path = %path.display(),
                "Metrolink GTFS-RT position file missing; coordinate filtering disabled"
            );
            Vec::new()
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("Cannot read Metrolink position file {}", path.display()));
        }
    };

    tracing::info!(path = %path.display(), count = rules.len(), "Loaded Metrolink GTFS-RT position exclusions");
    INVALID_GTFS_RT_POSITIONS
        .set(rules)
        .map_err(|_| anyhow::anyhow!("Metrolink position exclusions already initialized"))
}

fn parse_invalid_positions(contents: &str) -> anyhow::Result<Vec<(f32, f32)>> {
    let mut positions = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        let line = line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        let (latitude, longitude) = line
            .split_once(',')
            .with_context(|| format!("Line {}: expected latitude,longitude", index + 1))?;
        let latitude: f32 = latitude
            .trim()
            .parse()
            .with_context(|| format!("Line {}: invalid latitude", index + 1))?;
        let longitude: f32 = longitude
            .trim()
            .parse()
            .with_context(|| format!("Line {}: invalid longitude", index + 1))?;
        if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
            anyhow::bail!("Line {}: coordinates outside valid ranges", index + 1);
        }
        positions.push((latitude, longitude));
    }
    Ok(positions)
}

fn clear_invalid_positions(feed: &mut FeedMessage, rules: &[(f32, f32)]) {
    if rules.is_empty() {
        return;
    }

    for entity in &mut feed.entity {
        if let Some(vehicle) = &mut entity.vehicle {
            if vehicle.position.as_ref().is_some_and(|position| {
                rules.contains(&(position.latitude, position.longitude))
            }) {
                // Keep the entity, trip, vehicle descriptor and timestamp.
                vehicle.position = None;
            }
        }
    }
}

/// Only primary GTFS-RT positions are filtered, before compact storage and PTC supplementation.
pub fn filter_gtfs_rt_vehicle_positions(
    mut feed: FeedMessage,
    realtime_feed_id: &str,
) -> FeedMessage {
    if realtime_feed_id == METROLINK_FEED_ID {
        let rules = INVALID_GTFS_RT_POSITIONS
            .get()
            .expect("Metrolink position exclusions must be loaded at Aspen startup");
        clear_invalid_positions(&mut feed, rules);
    }
    feed
}

pub fn vehicle_pos_supplement(
    pos_aspenised: AspenisedVehiclePosition,
    fetch_supplemental_data_positions_metrolink: &Option<AHashMap<CompactString, MetrolinkPos>>,
    chateau_id: &str,
) -> AspenisedVehiclePosition {
    let mut pos_aspenised = pos_aspenised;

    match chateau_id {
        "metrolinktrains" => match fetch_supplemental_data_positions_metrolink {
            Some(supp_metrolink_data) => {
                if let Some(vehicle_ids) = &pos_aspenised.vehicle {
                    if let Some(vehicle_id) = &vehicle_ids.id {
                        if let Some(metrolink_pos) = supp_metrolink_data.get(vehicle_id.as_str()) {
                            if pos_aspenised.position.is_none() {
                                println!("Set new pos for Metrolink {}", vehicle_id);
                                pos_aspenised.position = Some(CatenaryRtVehiclePosition {
                                    latitude: metrolink_pos.lat,
                                    longitude: metrolink_pos.lon,
                                    bearing: None,
                                    odometer: None,
                                    speed: Some(metrolink_pos.speed),
                                });
                            }
                        }
                    }
                }
            }
            None => {}
        },
        _ => {}
    }

    pos_aspenised
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(latitude: f32, longitude: f32) -> CatenaryRtVehiclePosition {
        CatenaryRtVehiclePosition {
            latitude,
            longitude,
            bearing: None,
            odometer: None,
            speed: Some(0.0),
        }
    }

    fn vehicle(position: Option<CatenaryRtVehiclePosition>) -> AspenisedVehiclePosition {
        AspenisedVehiclePosition {
            trip: None,
            vehicle: Some(AspenisedVehicleDescriptor {
                id: Some("M100".to_string()),
                label: Some("100".to_string()),
                license_plate: None,
                wheelchair_accessible: None,
            }),
            position,
            timestamp: Some(123),
            route_type: 2,
            current_stop_sequence: None,
            current_status: None,
            congestion_level: None,
            occupancy_status: None,
            occupancy_percentage: None,
            consist: None,
        }
    }

    fn supplemental(lat: f32, lon: f32) -> Option<AHashMap<CompactString, MetrolinkPos>> {
        let symbol = CompactString::new("M100");
        let mut positions = AHashMap::new();
        positions.insert(
            symbol.clone(),
            MetrolinkPos {
                lat,
                lon,
                speed: 4.0,
                symbol,
            },
        );
        Some(positions)
    }

    use gtfs_realtime::{FeedEntity, Position, TripDescriptor, VehicleDescriptor, VehiclePosition};

    const REPORTED_INVALID_POSITIONS: [(f32, f32); 4] = [
        (33.5539, -117.675),
        (34.099503, -117.29602),
        (34.250793, -119.205025),
        (34.05573, -118.23351),
    ];

    fn shipped_rules() -> Vec<(f32, f32)> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(DEFAULT_INVALID_POSITIONS_FILE);
        parse_invalid_positions(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn gtfs_feed(latitude: f32, longitude: f32) -> FeedMessage {
        FeedMessage {
            header: gtfs_realtime::FeedHeader {
                gtfs_realtime_version: "2.0".to_string(),
                timestamp: Some(123),
                ..Default::default()
            },
            entity: vec![FeedEntity {
                id: "entity-M100".to_string(),
                vehicle: Some(VehiclePosition {
                    trip: Some(TripDescriptor {
                        trip_id: Some("trip-100".to_string()),
                        ..Default::default()
                    }),
                    vehicle: Some(VehicleDescriptor {
                        id: Some("M100".to_string()),
                        ..Default::default()
                    }),
                    position: Some(Position {
                        latitude,
                        longitude,
                        ..Default::default()
                    }),
                    timestamp: Some(123),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        }
    }

    #[test]
    fn shipped_file_contains_all_reported_invalid_positions() {
        let rules = shipped_rules();
        for pair in REPORTED_INVALID_POSITIONS {
            assert!(rules.contains(&pair), "{pair:?}");
        }
    }

    #[test]
    fn parses_external_coordinates_and_comments() {
        let rules = parse_invalid_positions("# comment\r\n\r\n33.5539, -117.675 # depot\r\n1,2\n").unwrap();
        assert_eq!(rules, vec![(33.5539, -117.675), (1.0, 2.0)]);
        assert!(parse_invalid_positions("# intentionally empty").unwrap().is_empty());
        for invalid in ["1", "1,2,3", "bad,0", "NaN,0", "91,0", "0,-181", "0,inf"] {
            assert!(parse_invalid_positions(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn metrolink_discards_exact_invalid_primary_positions_only() {
        let rules = shipped_rules();
        for (lat, lon) in REPORTED_INVALID_POSITIONS {
            let mut feed = gtfs_feed(lat, lon);
            let mut expected = feed.clone();
            expected.entity[0].vehicle.as_mut().unwrap().position = None;
            clear_invalid_positions(&mut feed, &rules);
            assert_eq!(feed, expected, "{lat}, {lon}");

            // Missing positions are not altered by repeated filtering.
            clear_invalid_positions(&mut feed, &rules);
            assert_eq!(feed, expected);
        }
    }

    #[test]
    fn metrolink_keeps_nearby_primary_positions() {
        let rules = shipped_rules();
        for (lat, lon) in REPORTED_INVALID_POSITIONS {
            for (latitude, longitude) in [(lat + 0.0001, lon), (lat, lon - 0.0001)] {
                let mut feed = gtfs_feed(latitude, longitude);
                let expected = feed.clone();
                clear_invalid_positions(&mut feed, &rules);
                assert_eq!(feed, expected);
            }
        }
    }

    #[test]
    fn other_feeds_keep_the_same_coordinates() {
        for (lat, lon) in REPORTED_INVALID_POSITIONS {
            let feed = gtfs_feed(lat, lon);
            assert_eq!(
                filter_gtfs_rt_vehicle_positions(feed.clone(), "f-amtrak~rt"),
                feed
            );
        }
    }

    #[test]
    fn supplemental_ptc_positions_are_not_filtered() {
        let rules = shipped_rules();
        for (lat, lon) in REPORTED_INVALID_POSITIONS {
            let mut feed = gtfs_feed(lat, lon);
            clear_invalid_positions(&mut feed, &rules);
            assert!(feed.entity[0].vehicle.as_ref().unwrap().position.is_none());

            let result = vehicle_pos_supplement(
                vehicle(None),
                &supplemental(lat, lon),
                "metrolinktrains",
            );
            let pos = result.position.unwrap();
            assert_eq!((pos.latitude, pos.longitude), (lat, lon));
            assert_eq!(pos.speed, Some(4.0));
            assert_eq!(result.timestamp, Some(123));

            let result = vehicle_pos_supplement(
                vehicle(None),
                &supplemental(lat + 0.0001, lon),
                "metrolinktrains",
            );
            let pos = result.position.unwrap();
            assert_eq!((pos.latitude, pos.longitude), (lat + 0.0001, lon));
        }
    }

    #[test]
    fn valid_primary_position_takes_precedence_over_ptc() {
        let result = vehicle_pos_supplement(
            vehicle(Some(position(34.1, -118.2))),
            &supplemental(33.5539, -117.675),
            "metrolinktrains",
        );
        let pos = result.position.unwrap();
        assert_eq!((pos.latitude, pos.longitude), (34.1, -118.2));
        assert_eq!(pos.speed, Some(0.0));
    }
}

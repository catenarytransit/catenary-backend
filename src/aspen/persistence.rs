use catenary::aspen_dataset::AspenisedData;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

pub fn save_chateau_data(
    chateau_id: &str,
    data: &AspenisedData,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = "data/aspen_data";
    std::fs::create_dir_all(dir)?;

    let file_path = format!("{}/{}.bin.zlib", dir, chateau_id);
    let temp_file_path = format!("{}/{}.bin.zlib.tmp", dir, chateau_id);

    let file = File::create(&temp_file_path)?;
    let writer = BufWriter::new(file);
    let mut encoder = flate2::write::ZlibEncoder::new(writer, flate2::Compression::default());

    let bytes = catenary::bincode_serialize(data)?;
    encoder.write_all(&bytes)?;

    encoder.finish()?;

    std::fs::rename(temp_file_path, file_path)?;

    Ok(())
}

pub fn load_chateau_data(
    chateau_id: &str,
) -> Result<Option<AspenisedData>, Box<dyn std::error::Error + Send + Sync>> {
    let file_path = format!("data/aspen_data/{}.bin.zlib", chateau_id);
    let path = Path::new(&file_path);

    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut decoder = flate2::read::ZlibDecoder::new(reader);
    let mut buffer = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut buffer)?;

    let data: AspenisedData = catenary::bincode_deserialize(&buffer)?;

    Ok(Some(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use catenary::aspen_dataset::{
        AspenisedData, CompressedTripInternalCache, ItineraryPatternInternalCache,
    };
    use std::collections::HashMap;

    #[test]
    fn test_save_and_load() {
        let chateau_id = "test_chateau";
        let data = AspenisedData {
            vehicle_positions: Default::default(),
            vehicle_routes_cache: Default::default(),
            vehicle_routes_cache_hash: 0,
            vehicle_label_to_gtfs_id: Default::default(),
            trip_updates: Default::default(),
            trip_updates_lookup_by_trip_id_to_trip_update_ids: Default::default(),
            trip_updates_lookup_by_route_id_to_trip_update_ids: Default::default(),
            aspenised_alerts: Default::default(),
            impacted_routes_alerts: Default::default(),
            impacted_stops_alerts: Default::default(),
            impacted_trips_alerts: Default::default(),
            trip_id_to_vehicle_gtfs_rt_id: Default::default(),
            last_updated_time_ms: 1234567890,
            itinerary_pattern_internal_cache: ItineraryPatternInternalCache::new(),
            compressed_trip_internal_cache: CompressedTripInternalCache::new(),
            stop_id_to_stop: Default::default(),
            shape_id_to_shape: Default::default(),
            trip_modifications: Default::default(),
            trip_id_to_trip_modification_ids: Default::default(),
            stop_id_to_trip_modification_ids: Default::default(),
            stop_id_to_non_scheduled_trip_ids: Default::default(),
        };

        save_chateau_data(chateau_id, &data).unwrap();
        let loaded_data = load_chateau_data(chateau_id).unwrap().unwrap();

        assert_eq!(data.last_updated_time_ms, loaded_data.last_updated_time_ms);
    }

    #[test]
    fn test_skip_itinerary_pattern_internal_cache() {
        let chateau_id = "test_skip_cache";
        let mut data = AspenisedData {
            vehicle_positions: Default::default(),
            vehicle_routes_cache: Default::default(),
            vehicle_routes_cache_hash: 0,
            vehicle_label_to_gtfs_id: Default::default(),
            trip_updates: Default::default(),
            trip_updates_lookup_by_trip_id_to_trip_update_ids: Default::default(),
            trip_updates_lookup_by_route_id_to_trip_update_ids: Default::default(),
            aspenised_alerts: Default::default(),
            impacted_routes_alerts: Default::default(),
            impacted_stops_alerts: Default::default(),
            impacted_trips_alerts: Default::default(),
            trip_id_to_vehicle_gtfs_rt_id: Default::default(),
            last_updated_time_ms: 1234567890,
            itinerary_pattern_internal_cache: ItineraryPatternInternalCache::new(),
            compressed_trip_internal_cache: CompressedTripInternalCache::new(),
            stop_id_to_stop: Default::default(),
            shape_id_to_shape: Default::default(),
            trip_modifications: Default::default(),
            trip_id_to_trip_modification_ids: Default::default(),
            stop_id_to_trip_modification_ids: Default::default(),
            stop_id_to_non_scheduled_trip_ids: Default::default(),
        };

        // Populate the cache with some dummy data
        data.itinerary_pattern_internal_cache
            .itinerary_patterns
            .insert(
                "test_pattern".to_string(),
                (
                    catenary::models::ItineraryPatternMeta {
                        onestop_feed_id: "test_feed".to_string(),
                        attempt_id: "test_attempt".to_string(),
                        trip_ids: vec![],
                        itinerary_pattern_id: "test_pattern".to_string(),
                        chateau: "test_chateau".to_string(),
                        trip_headsign: None,
                        trip_headsign_translations: None,
                        shape_id: None,
                        timezone: "UTC".to_string(),
                        route_id: "test_route".into(),
                        direction_pattern_id: Some("test_direction".to_string()),
                    },
                    vec![],
                ),
            );

        save_chateau_data(chateau_id, &data).unwrap();
        let loaded_data = load_chateau_data(chateau_id).unwrap().unwrap();

        // Verify that the cache is empty in the loaded data
        assert!(
            loaded_data
                .itinerary_pattern_internal_cache
                .itinerary_patterns
                .is_empty()
        );

        // Verify other data is preserved
        assert_eq!(data.last_updated_time_ms, loaded_data.last_updated_time_ms);
    }
}

//Assisted-by: Gemini 3 via Google Antigravity

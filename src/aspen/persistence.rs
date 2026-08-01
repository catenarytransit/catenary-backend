use catenary::aspen_dataset::AspenisedData;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

#[derive(serde::Serialize)]
struct AspenStaticTrajectoryDataRef<'a> {
    geometries: &'a Vec<catenary::aspen_dataset::PackedGeometry>,
    patterns: &'a Vec<catenary::aspen_dataset::PatternGeometry>,
    rtree_by_route_type:
        &'a ahash::AHashMap<i16, rstar::RTree<catenary::aspen_dataset::PatternBBox>>,
}

#[derive(serde::Serialize)]
struct AspenRealtimeTrajectoryDataRef<'a> {
    trajectories: &'a Vec<catenary::aspen_dataset::TrajectoryInstance>,
    pattern_to_trajectories: &'a Vec<Vec<u32>>,
}

fn encode_into_writer<T, W>(value: &T, writer: &mut W) -> Result<usize, bincode::error::EncodeError>
where
    T: serde::Serialize,
    W: Write,
{
    use bincode::config::{Configuration, Fixint, LittleEndian};

    let config: Configuration<LittleEndian, Fixint> = bincode::config::legacy();
    bincode::serde::encode_into_std_write(value, writer, config)
}

fn decode_from_reader<T, R>(reader: &mut R) -> Result<T, bincode::error::DecodeError>
where
    T: serde::de::DeserializeOwned,
    R: Read,
{
    use bincode::config::{Configuration, Fixint, LittleEndian};

    let config: Configuration<LittleEndian, Fixint> = bincode::config::legacy();
    bincode::serde::decode_from_std_read(reader, config)
}

/// Removes static trajectory entries that are no longer reachable from any
/// current trajectory and remaps every pattern, geometry, and R-tree index.
pub fn compact_trajectory_store(
    store: catenary::aspen_dataset::AspenTrajectoryStore,
) -> (catenary::aspen_dataset::AspenTrajectoryStore, bool) {
    use catenary::aspen_dataset::{AspenTrajectoryStore, PatternBBox};

    let AspenTrajectoryStore {
        geometries,
        patterns,
        mut trajectories,
        pattern_to_trajectories: _,
        rtree_by_route_type,
    } = store;

    let original_geometry_count = geometries.len();
    let original_pattern_count = patterns.len();
    let original_bbox_count = rtree_by_route_type
        .values()
        .map(|tree| tree.size())
        .sum::<usize>();

    let mut used_patterns = vec![false; patterns.len()];
    trajectories.retain(|trajectory| {
        let pattern_id = trajectory.pattern_id as usize;
        let Some(pattern) = patterns.get(pattern_id) else {
            return false;
        };

        if pattern.geometry_id as usize >= geometries.len() {
            return false;
        }

        used_patterns[pattern_id] = true;
        true
    });

    let mut pattern_remap = vec![None; patterns.len()];
    let mut next_pattern_id = 0_u32;
    for (old_pattern_id, is_used) in used_patterns.iter().enumerate() {
        if *is_used {
            pattern_remap[old_pattern_id] = Some(next_pattern_id);
            next_pattern_id += 1;
        }
    }

    let mut used_geometries = vec![false; geometries.len()];
    for (is_used, pattern) in used_patterns.iter().zip(patterns.iter()) {
        if *is_used {
            used_geometries[pattern.geometry_id as usize] = true;
        }
    }

    let mut geometry_remap = vec![None; geometries.len()];
    let mut compacted_geometries =
        Vec::with_capacity(used_geometries.iter().filter(|is_used| **is_used).count());
    for (old_geometry_id, geometry) in geometries.into_iter().enumerate() {
        if used_geometries[old_geometry_id] {
            geometry_remap[old_geometry_id] = Some(compacted_geometries.len() as u32);
            compacted_geometries.push(geometry);
        }
    }

    let mut compacted_patterns = Vec::with_capacity(next_pattern_id as usize);
    for (old_pattern_id, mut pattern) in patterns.into_iter().enumerate() {
        if pattern_remap[old_pattern_id].is_none() {
            continue;
        }

        pattern.geometry_id = geometry_remap[pattern.geometry_id as usize]
            .expect("used trajectory pattern must have a valid geometry");
        compacted_patterns.push(pattern);
    }

    for trajectory in &mut trajectories {
        trajectory.pattern_id = pattern_remap[trajectory.pattern_id as usize]
            .expect("retained trajectory must have a retained pattern");
    }

    let mut compacted_pattern_to_trajectories = vec![Vec::new(); compacted_patterns.len()];
    for (trajectory_id, trajectory) in trajectories.iter().enumerate() {
        compacted_pattern_to_trajectories[trajectory.pattern_id as usize]
            .push(trajectory_id as u32);
    }

    let mut compacted_rtree_by_route_type =
        ahash::AHashMap::with_capacity(rtree_by_route_type.len());
    let mut compacted_bbox_count = 0;
    for (route_type, tree) in rtree_by_route_type {
        let boxes = tree
            .iter()
            .filter_map(|bbox| {
                let new_pattern_id = pattern_remap
                    .get(bbox.pattern_id as usize)
                    .copied()
                    .flatten()?;
                let mut compacted_bbox: PatternBBox = (*bbox).clone();
                compacted_bbox.pattern_id = new_pattern_id;
                Some(compacted_bbox)
            })
            .collect::<Vec<_>>();

        compacted_bbox_count += boxes.len();
        if !boxes.is_empty() {
            compacted_rtree_by_route_type.insert(route_type, rstar::RTree::bulk_load(boxes));
        }
    }

    let static_changed = original_geometry_count != compacted_geometries.len()
        || original_pattern_count != compacted_patterns.len()
        || original_bbox_count != compacted_bbox_count;

    (
        AspenTrajectoryStore {
            geometries: compacted_geometries,
            patterns: compacted_patterns,
            trajectories,
            pattern_to_trajectories: compacted_pattern_to_trajectories,
            rtree_by_route_type: compacted_rtree_by_route_type,
        },
        static_changed,
    )
}

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

pub fn save_trajectory_data(
    chateau_id: &str,
    data: &catenary::aspen_dataset::AspenTrajectoryStore,
    static_changed: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = "data/aspen_trajectories";
    std::fs::create_dir_all(dir)?;

    let static_file_path = format!("{}/{}_static.bin.zlib", dir, chateau_id);
    let static_path = Path::new(&static_file_path);

    if static_changed || !static_path.exists() {
        let static_temp = format!("{}/{}_static.bin.zlib.tmp", dir, chateau_id);
        let static_data = AspenStaticTrajectoryDataRef {
            geometries: &data.geometries,
            patterns: &data.patterns,
            rtree_by_route_type: &data.rtree_by_route_type,
        };
        let file = File::create(&static_temp)?;
        let writer = BufWriter::new(file);
        let mut encoder = flate2::write::ZlibEncoder::new(writer, flate2::Compression::default());
        encode_into_writer(&static_data, &mut encoder)?;
        encoder.finish()?;
        std::fs::rename(static_temp, static_file_path)?;
    }

    let rt_file_path = format!("{}/{}_realtime.bin.zlib", dir, chateau_id);
    let rt_temp = format!("{}/{}_realtime.bin.zlib.tmp", dir, chateau_id);
    let rt_data = AspenRealtimeTrajectoryDataRef {
        trajectories: &data.trajectories,
        pattern_to_trajectories: &data.pattern_to_trajectories,
    };
    let file = File::create(&rt_temp)?;
    let writer = BufWriter::new(file);
    let mut encoder = flate2::write::ZlibEncoder::new(writer, flate2::Compression::default());
    encode_into_writer(&rt_data, &mut encoder)?;
    encoder.finish()?;
    std::fs::rename(rt_temp, rt_file_path)?;

    Ok(())
}

pub fn load_trajectory_data(
    chateau_id: &str,
) -> Result<
    Option<catenary::aspen_dataset::AspenTrajectoryStore>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let dir = "data/aspen_trajectories";
    let static_file_path = format!("{}/{}_static.bin.zlib", dir, chateau_id);
    let rt_file_path = format!("{}/{}_realtime.bin.zlib", dir, chateau_id);

    let static_path = Path::new(&static_file_path);
    let rt_path = Path::new(&rt_file_path);

    if !static_path.exists() && !rt_path.exists() {
        let old_file_path = format!("{}/{}.bin.zlib", dir, chateau_id);
        let old_path = Path::new(&old_file_path);
        if old_path.exists() {
            let file = File::open(old_path)?;
            let reader = BufReader::new(file);
            let mut decoder = flate2::read::ZlibDecoder::new(reader);
            let data: catenary::aspen_dataset::AspenTrajectoryStore =
                decode_from_reader(&mut decoder)?;
            let (data, static_changed) = compact_trajectory_store(data);
            if static_changed {
                if let Err(error) = save_trajectory_data(chateau_id, &data, true) {
                    eprintln!(
                        "Failed to rewrite compacted trajectory data for {}: {}",
                        chateau_id, error
                    );
                }
            }
            return Ok(Some(data));
        }
        return Ok(None);
    }

    let static_data = if static_path.exists() {
        let file = File::open(static_path)?;
        let reader = BufReader::new(file);
        let mut decoder = flate2::read::ZlibDecoder::new(reader);
        decode_from_reader::<catenary::aspen_dataset::AspenStaticTrajectoryData, _>(&mut decoder)?
    } else {
        catenary::aspen_dataset::AspenStaticTrajectoryData {
            geometries: Vec::new(),
            patterns: Vec::new(),
            rtree_by_route_type: ahash::AHashMap::new(),
        }
    };

    let rt_data = if rt_path.exists() {
        let file = File::open(rt_path)?;
        let reader = BufReader::new(file);
        let mut decoder = flate2::read::ZlibDecoder::new(reader);
        decode_from_reader::<catenary::aspen_dataset::AspenRealtimeTrajectoryData, _>(&mut decoder)?
    } else {
        catenary::aspen_dataset::AspenRealtimeTrajectoryData {
            trajectories: Vec::new(),
            pattern_to_trajectories: Vec::new(),
        }
    };

    let store = catenary::aspen_dataset::AspenTrajectoryStore {
        geometries: static_data.geometries,
        patterns: static_data.patterns,
        trajectories: rt_data.trajectories,
        pattern_to_trajectories: rt_data.pattern_to_trajectories,
        rtree_by_route_type: static_data.rtree_by_route_type,
    };
    let (store, static_changed) = compact_trajectory_store(store);

    if static_changed {
        println!("Compacted persisted trajectory data for {}", chateau_id);
        if let Err(error) = save_trajectory_data(chateau_id, &store, true) {
            eprintln!(
                "Failed to rewrite compacted trajectory data for {}: {}",
                chateau_id, error
            );
        }
    }

    Ok(Some(store))
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
            vehicle_positions_rtree_by_route_type: Default::default(),
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
            stop_id_to_parent_id: Default::default(),
            parent_id_to_children_ids: Default::default(),
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
            vehicle_positions_rtree_by_route_type: Default::default(),
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
            stop_id_to_parent_id: Default::default(),
            parent_id_to_children_ids: Default::default(),
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
                        row_count: 0,
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

    #[test]
    fn test_save_and_load_trajectory() {
        let chateau_id = "test_trajectory_chateau";
        let store = catenary::aspen_dataset::AspenTrajectoryStore {
            geometries: Default::default(),
            patterns: Default::default(),
            trajectories: Default::default(),
            pattern_to_trajectories: Default::default(),
            rtree_by_route_type: Default::default(),
        };

        save_trajectory_data(chateau_id, &store, true).unwrap();
        let loaded_store = load_trajectory_data(chateau_id).unwrap().unwrap();

        assert_eq!(
            store.pattern_to_trajectories.len(),
            loaded_store.pattern_to_trajectories.len()
        );
    }

    #[test]
    fn test_compact_trajectory_store_removes_unused_static_data() {
        use catenary::aspen_dataset::{
            AspenTrajectoryStore, PackedGeometry, PatternBBox, PatternGeometry, TrajectoryInstance,
        };
        use compact_str::CompactString;

        let geometries = (0_i32..3)
            .map(|coordinate| PackedGeometry {
                coordinates: vec![[coordinate, coordinate]].into_boxed_slice(),
            })
            .collect::<Vec<_>>();
        let patterns = (0_u32..3)
            .map(|pattern_id| PatternGeometry {
                pattern_id_str: format!("pattern_{pattern_id}").into(),
                route_id: "route".into(),
                mode: "rail".into(),
                geometry_id: pattern_id,
                segments: Vec::new().into_boxed_slice(),
                distance: 0.0,
            })
            .collect::<Vec<_>>();
        let trajectories = vec![
            TrajectoryInstance {
                pattern_id: 0,
                trip_id: CompactString::new("trip_0"),
                start_time: None,
                service_date: None,
                trip_short_name: None,
                stops: Vec::new().into_boxed_slice(),
            },
            TrajectoryInstance {
                pattern_id: 2,
                trip_id: CompactString::new("trip_2"),
                start_time: None,
                service_date: None,
                trip_short_name: None,
                stops: Vec::new().into_boxed_slice(),
            },
        ];
        let mut rtree_by_route_type = ahash::AHashMap::new();
        rtree_by_route_type.insert(
            2,
            rstar::RTree::bulk_load(
                (0_u32..3)
                    .map(|pattern_id| PatternBBox {
                        pattern_id,
                        min_lon: pattern_id as f64,
                        min_lat: pattern_id as f64,
                        max_lon: pattern_id as f64 + 1.0,
                        max_lat: pattern_id as f64 + 1.0,
                    })
                    .collect(),
            ),
        );

        let (store, static_changed) = compact_trajectory_store(AspenTrajectoryStore {
            geometries,
            patterns,
            trajectories,
            pattern_to_trajectories: vec![vec![0], Vec::new(), vec![1]],
            rtree_by_route_type,
        });

        assert!(static_changed);
        assert_eq!(store.geometries.len(), 2);
        assert_eq!(store.patterns.len(), 2);
        assert_eq!(store.patterns[0].pattern_id_str.as_str(), "pattern_0");
        assert_eq!(store.patterns[1].pattern_id_str.as_str(), "pattern_2");
        assert_eq!(store.patterns[0].geometry_id, 0);
        assert_eq!(store.patterns[1].geometry_id, 1);
        assert_eq!(store.trajectories[0].pattern_id, 0);
        assert_eq!(store.trajectories[1].pattern_id, 1);
        assert_eq!(store.pattern_to_trajectories, vec![vec![0], vec![1]]);

        let mut bbox_pattern_ids = store.rtree_by_route_type[&2]
            .iter()
            .map(|bbox| bbox.pattern_id)
            .collect::<Vec<_>>();
        bbox_pattern_ids.sort_unstable();
        assert_eq!(bbox_pattern_ids, vec![0, 1]);
    }
}

//Assisted-by: Gemini 3 via Google Antigravity

pub struct GtfsRtQualityCheckResults {
    pub entities_id_using_timestamp_from_global: usize,
    pub total_entity_count: usize,
    pub vehicles_at_null_island: usize,
}

pub fn validate_gtfs_rt(input: &gtfs_realtime::FeedMessage) -> GtfsRtQualityCheckResults {
    let total_entity_count = input.entity.len();
    let mut entities_id_using_timestamp_from_global: usize = 0;
    let mut vehicles_at_null_island: usize = 0;

    let global_timestamp = input.header.timestamp;
    let global_timestamp_string =
        global_timestamp.map(|global_timestamp| global_timestamp.to_string());

    for entity in &input.entity {
        if let Some(global_timestamp_string) = &global_timestamp_string {
            if entity.id.contains(global_timestamp_string) {
                entities_id_using_timestamp_from_global += 1;
            }
        }

        if let Some(vehicle) = &entity.vehicle {
            if let Some(position) = &vehicle.position {
                if f32::abs(0.0 - position.latitude) < 0.01
                    && f32::abs(0.0 - position.longitude) < 0.01
                {
                    vehicles_at_null_island += 1;
                }
            }
        }
    }

    GtfsRtQualityCheckResults {
        entities_id_using_timestamp_from_global,
        total_entity_count,
        vehicles_at_null_island,
    }
}

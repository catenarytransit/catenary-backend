use crate::aspen_dataset::GtfsRtType;
use crate::compact_rt_v2::CompactFeedMessageV2;
use scc::HashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// GTFS-RT publisher header timestamps in Unix seconds, not ingestion times.
/// A missing category or a header without a timestamp is represented as null in JSON.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GtfsRtTimestamps {
    pub vehicle_positions: Option<u64>,
    pub trip_updates: Option<u64>,
    pub alerts: Option<u64>,
}

pub async fn get_gtfs_rt_timestamps(
    store: &HashMap<(String, GtfsRtType), Arc<CompactFeedMessageV2>>,
    realtime_feed_id: &str,
) -> GtfsRtTimestamps {
    let timestamp = async |feed_type| {
        store
            .get_async(&(realtime_feed_id.to_owned(), feed_type))
            .await
            .and_then(|entry| entry.get().header.timestamp)
    };

    GtfsRtTimestamps {
        vehicle_positions: timestamp(GtfsRtType::VehiclePositions).await,
        trip_updates: timestamp(GtfsRtType::TripUpdates).await,
        alerts: timestamp(GtfsRtType::Alerts).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtfs_realtime::{FeedHeader, FeedMessage};

    fn feed(timestamp: Option<u64>) -> Arc<CompactFeedMessageV2> {
        Arc::new(CompactFeedMessageV2::from_feed_message(FeedMessage {
            header: FeedHeader {
                gtfs_realtime_version: "2.0".to_owned(),
                timestamp,
                ..Default::default()
            },
            entity: vec![],
        }))
    }

    #[tokio::test]
    async fn missing_feed_serializes_all_categories_as_null() {
        let result = get_gtfs_rt_timestamps(&HashMap::new(), "missing").await;
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            serde_json::json!({
                "vehicle_positions": null,
                "trip_updates": null,
                "alerts": null
            })
        );
    }

    #[tokio::test]
    async fn reads_each_category_and_isolates_feed_ids() {
        let store = HashMap::new();
        for (category, timestamp) in [
            (GtfsRtType::VehiclePositions, 100),
            (GtfsRtType::TripUpdates, 200),
            (GtfsRtType::Alerts, 0),
        ] {
            store
                .insert_async(("test".to_owned(), category), feed(Some(timestamp)))
                .await
                .unwrap();
        }
        store
            .insert_async(("other".to_owned(), GtfsRtType::TripUpdates), feed(Some(999)))
            .await
            .unwrap();
        assert_eq!(
            get_gtfs_rt_timestamps(&store, "test").await,
            GtfsRtTimestamps {
                vehicle_positions: Some(100),
                trip_updates: Some(200),
                alerts: Some(0),
            }
        );

        // Replacing a timestamped feed with a header without a timestamp must
        // clear the reported value rather than use an older cached timestamp.
        store
            .remove_async(&("test".to_owned(), GtfsRtType::TripUpdates))
            .await;
        store
            .insert_async(("test".to_owned(), GtfsRtType::TripUpdates), feed(None))
            .await
            .unwrap();
        store
            .remove_async(&("test".to_owned(), GtfsRtType::Alerts))
            .await;
        assert_eq!(
            get_gtfs_rt_timestamps(&store, "test").await,
            GtfsRtTimestamps {
                vehicle_positions: Some(100),
                trip_updates: None,
                alerts: None,
            }
        );
    }
}

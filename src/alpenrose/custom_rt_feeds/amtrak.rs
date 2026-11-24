use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::RealtimeFeedFetch;

pub const FEED_ID: &str = "f-amtrak~rt";
const STATIC_GTFS_URL: &str = "https://github.com/catenarytransit/amtrak-schedule-fixer/releases/download/latest/amtrak-gtfs-noshapes-fixed.zip";
const REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RETRY_INTERVAL: Duration = Duration::from_secs(60);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);

pub type AmtrakGtfsCache = Arc<RwLock<Option<Arc<gtfs_structures::Gtfs>>>>;

// Hold the assignment read lock only while publishing, so a worker that lost
// the feed cannot install a schedule downloaded for its previous assignment.
// Readers can retain an Arc to the old schedule until they finish converting.
async fn publish_if_assigned<T, A>(
    assignments: &RwLock<HashMap<String, A>>,
    cache: &RwLock<Option<Arc<T>>>,
    replacement: T,
) -> bool {
    let replacement = Arc::new(replacement);
    let assigned = assignments.read().await;
    if !assigned.contains_key(FEED_ID) {
        return false;
    }

    let previous = cache.write().await.replace(replacement);
    drop(assigned);
    // Destruction of a potentially large schedule must not hold the cache lock.
    drop(previous);
    true
}

// Exactly one refresh loop is managed by the worker for the duration of its
// Amtrak assignment. Download and parse without taking the cache write lock.
pub async fn refresh_static_gtfs(
    assignments: Arc<RwLock<HashMap<String, RealtimeFeedFetch>>>,
    cache: AmtrakGtfsCache,
) {
    loop {
        if !assignments.read().await.contains_key(FEED_ID) {
            return;
        }

        println!("Downloading Amtrak static GTFS...");
        let result = tokio::time::timeout(DOWNLOAD_TIMEOUT, async {
            gtfs_structures::GtfsReader::default()
                .read_shapes(false)
                .read_from_url_async(STATIC_GTFS_URL)
                .await
        })
        .await;

        let delay = match result {
            Ok(Ok(gtfs)) if gtfs.trips.is_empty() || gtfs.stops.is_empty() => {
                eprintln!("Amtrak static GTFS has no trips or stops; retaining previous schedule");
                RETRY_INTERVAL
            }
            Ok(Ok(gtfs)) => {
                if !publish_if_assigned(&assignments, &cache, gtfs).await {
                    return;
                }
                println!("Amtrak static GTFS refreshed successfully.");
                REFRESH_INTERVAL
            }
            Ok(Err(e)) => {
                eprintln!(
                    "Failed to download Amtrak GTFS: {}; retaining previous schedule",
                    e
                );
                RETRY_INTERVAL
            }
            Err(_) => {
                eprintln!("Amtrak GTFS download timed out; retaining previous schedule");
                RETRY_INTERVAL
            }
        };

        tokio::time::sleep(delay).await;
    }
}

use catenary::duration_since_unix_epoch;

use prost::Message;

pub async fn fetch_amtrak_data(
    realtime_feed_cache: std::sync::Arc<
        catenary::etcd_cache::EtcdCache<catenary::RealtimeFeedMetadataEtcd>,
    >,
    feed_id: &str,
    gtfs: &gtfs_structures::Gtfs,
    client: &reqwest::Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let fetch_assigned_node_meta = realtime_feed_cache.get(feed_id);

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let amtrak_gtfs_rt = amtrak_gtfs_rt::fetch_amtrak_gtfs_rt(gtfs, client).await;

        match amtrak_gtfs_rt {
            Ok(amtrak_gtfs_rt) => {
                //extract the binary data
                let vehicle_data = amtrak_gtfs_rt.vehicle_positions.encode_to_vec();
                let trip_data = amtrak_gtfs_rt.trip_updates.encode_to_vec();
                let alert_data = amtrak_gtfs_rt.alerts.encode_to_vec();

                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await?;

                let tarpc_send_to_aspen = aspen_client
                    .from_alpenrose(
                        tarpc::context::current(),
                        data.chateau_id.clone(),
                        String::from(feed_id),
                        Some(vehicle_data),
                        Some(trip_data),
                        Some(alert_data),
                        true,
                        true,
                        false,
                        Some(200),
                        Some(200),
                        Some(200),
                        duration_since_unix_epoch().as_millis() as u64,
                    )
                    .await;

                match tarpc_send_to_aspen {
                    Ok(_) => {
                        println!("Successfully sent Amtrak data sent to {}", feed_id);
                    }
                    Err(e) => {
                        eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch Amtrak data");
                eprintln!("{:?}", e);
            }
        }
    } else {
        println!("No assigned node found for Amtrak");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn assigned_worker_atomically_replaces_schedule() {
        let assignments = RwLock::new(HashMap::from([(FEED_ID.to_string(), ())]));
        let cache = RwLock::new(None);

        assert!(publish_if_assigned(&assignments, &cache, 1_u32).await);
        let in_flight = cache.read().await.clone().unwrap();
        assert!(publish_if_assigned(&assignments, &cache, 2_u32).await);

        assert_eq!(*in_flight, 1);
        assert_eq!(cache.read().await.as_deref(), Some(&2));
    }

    #[tokio::test]
    async fn unassigned_worker_cannot_publish_schedule() {
        let assignments = RwLock::new(HashMap::<String, ()>::new());
        let cache = RwLock::new(None);

        assert!(!publish_if_assigned(&assignments, &cache, 1_u32).await);
        assert!(cache.read().await.is_none());

        assignments.write().await.insert(FEED_ID.to_string(), ());
        assert!(publish_if_assigned(&assignments, &cache, 2_u32).await);

        assignments.write().await.remove(FEED_ID);
        assert!(!publish_if_assigned(&assignments, &cache, 3_u32).await);
        assert_eq!(cache.read().await.as_deref(), Some(&2));
    }
}

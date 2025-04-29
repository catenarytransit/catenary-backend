use catenary::agency_secret::KeyFormat;
use catenary::agency_secret::PasswordFormat;
use catenary::agency_secret::PasswordInfo;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::SelectableHelper;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::select_dsl::SelectDsl;
use diesel_async::RunQueryDsl;
use dmfr_dataset_reader::read_folders;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

#[derive(Serialize, Clone, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct RealtimeFeedFetch {
    pub feed_id: String,
    pub realtime_vehicle_positions: Option<String>,
    pub realtime_trip_updates: Option<String>,
    pub realtime_alerts: Option<String>,
    pub key_formats: Vec<KeyFormat>,
    pub passwords: Option<Vec<PasswordInfo>>,
    pub fetch_interval_ms: Option<i32>,
}

unsafe impl Send for RealtimeFeedFetch {}
unsafe impl Sync for RealtimeFeedFetch {}

pub async fn get_feed_metadata(
    arc_conn_pool: Arc<CatenaryPostgresPool>,
) -> Result<Vec<RealtimeFeedFetch>, Box<dyn Error + Sync + Send>> {
    //Get feed metadata from postgres
    //let dmfr_result = read_folders("./transitland-atlas/")?;

    //get everything out of realtime feeds table and realtime password tables

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    use catenary::schema::gtfs::realtime_feeds as realtime_feeds_table;

    let realtime_feeds = realtime_feeds_table::dsl::realtime_feeds
        .select(catenary::models::RealtimeFeed::as_select())
        .load::<catenary::models::RealtimeFeed>(conn)
        .await?;

    //format realtime feeds into HashMap

    let mut realtime_feeds_hashmap: HashMap<String, catenary::models::RealtimeFeed> =
        HashMap::new();

    for realtime_feed in realtime_feeds {
        let feed_id = realtime_feed.onestop_feed_id.clone();
        realtime_feeds_hashmap.insert(feed_id, realtime_feed);
    }

    let realtime_passwords = catenary::schema::gtfs::realtime_passwords::table
        .select(catenary::models::RealtimePasswordRow::as_select())
        .load::<catenary::models::RealtimePasswordRow>(conn)
        .await?;

    //format realtime passwords into HashMap

    let mut realtime_passwords_hashmap: HashMap<String, PasswordFormat> = HashMap::new();

    for realtime_password in realtime_passwords {
        let feed_id = realtime_password.onestop_feed_id.clone();
        let password_raw_json = realtime_password.passwords.clone();

        let password = password_raw_json.map(|password_format| {
            serde_json::from_value::<PasswordFormat>(password_format).unwrap()
        });

        if let Some(password) = password {
            realtime_passwords_hashmap.insert(feed_id, password);
        }
    }

    let mut realtime_feed_fetches: Vec<RealtimeFeedFetch> = Vec::new();

    for (feed_id, realtime_feed) in realtime_feeds_hashmap.iter() {
        let vehicles_url = match realtime_passwords_hashmap.get(feed_id) {
            Some(password_format) => match &password_format.override_realtime_vehicle_positions {
                Some(url) => Some(url.to_string()),
                None => realtime_feed
                    .realtime_vehicle_positions
                    .as_ref()
                    .map(|url| url.as_str().to_string()),
            },
            None => realtime_feed
                .realtime_vehicle_positions
                .as_ref()
                .map(|url| url.as_str().to_string()),
        };

        let trip_updates_url = match realtime_passwords_hashmap.get(feed_id) {
            Some(password_format) => match &password_format.override_realtime_trip_updates {
                Some(url) => Some(url.to_string()),
                None => realtime_feed
                    .realtime_trip_updates
                    .as_ref()
                    .map(|url| url.as_str().to_string()),
            },
            None => realtime_feed
                .realtime_trip_updates
                .as_ref()
                .map(|url| url.as_str().to_string()),
        };

        let alerts_url = match realtime_passwords_hashmap.get(feed_id) {
            Some(password_format) => match &password_format.override_alerts {
                Some(url) => Some(url.to_string()),
                None => realtime_feed
                    .realtime_alerts
                    .as_ref()
                    .map(|url| url.as_str().to_string()),
            },
            None => realtime_feed
                .realtime_alerts
                .as_ref()
                .map(|url| url.as_str().to_string()),
        };

        realtime_feed_fetches.push(RealtimeFeedFetch {
            feed_id: feed_id.clone(),
            realtime_vehicle_positions: vehicles_url,
            realtime_trip_updates: trip_updates_url,
            realtime_alerts: alerts_url,
            key_formats: match realtime_passwords_hashmap.get(feed_id) {
                Some(password_format) => password_format.key_formats.clone(),
                None => vec![],
            },
            passwords: realtime_passwords_hashmap
                .get(feed_id)
                .map(|password_format| password_format.passwords.clone()),
            fetch_interval_ms: match realtime_feeds_hashmap.get(feed_id) {
                Some(realtime_feed) => realtime_feed.fetch_interval_ms,
                None => None,
            },
        });
    }

    Ok(realtime_feed_fetches)
}

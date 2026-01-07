
use chrono::{DateTime, Duration, Utc};
use gtfs_realtime::{
    Alert,
    Entity,
    FeedEntity,
    FeedMessage,
    TranslatedString,
    TripDescriptor,
};
use scraper::{Html, Selector};
use std::str::FromStr;

pub async fn fetch_twitter_alerts(
    gtfs: &gtfs_structures::Gtfs,
    client: &reqwest::Client,
) -> Result<FeedMessage, Box<dyn std::error::Error + Sync + Send>> {
    let route_id = gtfs
        .routes
        .values()
        .find(|r| r.long_name == "Pacific Surfliner")
        .map(|r| r.id.clone())
        .ok_or("Pacific Surfliner route not found")?;

    let url = "https://x.com/pacsurfalerts";
    let html_content = client.get(url).send().await?.text().await?;
    let document = Html::parse_document(&html_content);

    let tweet_selector = Selector::parse("article").unwrap();
    let time_selector = Selector::parse("time").unwrap();
    let tweet_text_selector = Selector::parse("[data-testid=\"tweetText\"]").unwrap();

    let mut alerts = Vec::new();
    let twenty_four_hours_ago = Utc::now() - Duration::hours(24);

    for element in document.select(&tweet_selector) {
        let time_element = element.select(&time_selector).next();
        let tweet_text_element = element.select(&tweet_text_selector).next();

        if let (Some(time_el), Some(tweet_text_el)) = (time_element, tweet_text_element) {
            if let Some(datetime_str) = time_el.value().attr("datetime") {
                if let Ok(tweet_time) = DateTime::from_str(datetime_str) {
                    if tweet_time > twenty_four_hours_ago {
                        let tweet_text = tweet_text_el.text().collect::<String>();

                        let mut translation = TranslatedString::new();
                        translation.add_translation("en", &tweet_text);

                        let mut entity_selector = gtfs_realtime::EntitySelector::new();
                        entity_selector.set_route_id(route_id.clone());

                        let alert = Alert {
                            active_period: Vec::new(),
                            informed_entity: vec![entity_selector],
                            cause: Some(gtfs_realtime::alert::Cause::UnknownCause as i32),
                            effect: Some(gtfs_realtime::alert::Effect::UnknownEffect as i32),
                            url: None,
                            header_text: Some(translation.clone()),
                            description_text: Some(translation),
                            severity_level: None,
                            image: None,
                            image_alternative_text: None,
                            cause_detail: None,
                            effect_detail: None,
                        };

                        let feed_entity = FeedEntity {
                            id: format!("twitter:{}", tweet_time.timestamp()),
                            is_deleted: None,
                            trip_update: None,
                            vehicle: None,
                            alert: Some(alert),
                            shape: None,
                        };
                        alerts.push(feed_entity);
                    }
                }
            }
        }
    }

    let mut feed = FeedMessage::new();
    let mut header = gtfs_realtime::FeedHeader::new();
    header.set_gtfs_realtime_version("2.0".to_string());
    header.set_timestamp(Utc::now().timestamp() as u64);
    feed.set_header(header);
    feed.set_entity(alerts);

    Ok(feed)
}

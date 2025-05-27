use chrono::{TimeZone, Utc};
use chrono_tz::US::Pacific;
use futures::stream::{self, StreamExt};
use gtfs_realtime::FeedEntity;
use itertools::Itertools;
use scraper::Html;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::error::Error;
use std::io::ErrorKind;

pub type RawMetrolinkAlerts = Vec<RawMetrolinkEachRoute>;

pub const METROLINK_ALERTS_URL: &str = "https://metrolinktrains.com/advisories/getadvisories";

pub const GREETING: &str = "Good morning! We'll be posting your train status updates from 4:00 a.m. to 12:30 a.m. Be safe and have a great day!";
pub const GREETING_WEEKEND: &str = "Good morning! We'll be posting your train status updates from 6:00 a.m. to 11:00 p.m. Be safe and have a great day!";

pub const REMOVE_DELUSION: &str = "Please review OCTA bus routes connecting Laguna Niguel/Mission Viejo and Oceanside as alternatives.";
pub const REMOVE_YAP: &str = "We apologize that necessary infrastructure enhancements sometimes result in track closures that we know may be inconvenient for our riders. This work is needed to ensure the safety and reliability of the system.";

pub const ALERT_URL_PREFIX: &str = "https://metrolinktrains.com/news/alert-details-page/?alertId=";

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawMetrolinkEachRoute {
    #[serde(rename = "Line")]
    //Looks like Antelope Valley Line
    pub line: String,
    #[serde(rename = "LineAbbreviation")]
    //Looks like AV
    pub line_abbreviation: String,
    #[serde(rename = "ServiceAdvisories")]
    pub service_advisories: Vec<ServiceAdvisory>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAdvisory {
    #[serde(rename = "Id")]
    pub id: i64,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Line")]
    //looks like "VC"
    pub line: String,
    #[serde(rename = "Platform")]
    pub platform: String,
    #[serde(rename = "PlayTime")]
    pub play_time: Option<String>,
    #[serde(rename = "CreateDate")]
    pub create_date: String,
    #[serde(rename = "StartDateTime")]
    pub start_date_time: String,
    #[serde(rename = "ShortStartDateTime")]
    pub short_start_date_time: String,
    #[serde(rename = "EndDateTime")]
    pub end_date_time: Option<String>,
    #[serde(rename = "ShortEndDateTime")]
    pub short_end_date_time: Option<String>,
    #[serde(rename = "Timestamp")]
    pub timestamp: String,
    #[serde(rename = "Type")]
    pub type_field: String,
    #[serde(rename = "DetailsPage")]
    pub details_page: String,
    #[serde(rename = "AlertDetailsPage")]
    pub alert_details_page: Value,
    #[serde(rename = "DateRangeOutput")]
    pub date_range_output: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewAlert {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "Alert")]
    pub alert: NewAlertInner,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewAlertInner {
    #[serde(rename = "ActivePeriod")]
    pub active_period: Vec<NewAlertActivePeriod>,
    #[serde(rename = "InformedEntity")]
    pub informed_entity: Vec<NewAlertInformedEntitySelector>,
    #[serde(rename = "Cause")]
    pub cause: Option<i32>,
    #[serde(rename = "Effect")]
    pub effect: Option<i32>,
    #[serde(rename = "HeaderText")]
    pub header_text: Option<MetrolinkTranslatedString>,
    #[serde(rename = "DescriptionText")]
    pub description_text: Option<MetrolinkTranslatedString>,
    #[serde(rename = "TtsHeaderText")]
    pub tts_header_text: Option<MetrolinkTranslatedString>,
    #[serde(rename = "TtsDescriptionText")]
    pub tts_description_text: Option<MetrolinkTranslatedString>,
    #[serde(rename = "Url")]
    pub url: Option<MetrolinkTranslatedString>,
    #[serde(rename = "SeverityLevel")]
    pub severity_level: Option<i32>,
    #[serde(rename = "CauseDetail")]
    pub cause_detail: Option<MetrolinkTranslatedString>,
    #[serde(rename = "EffectDetail")]
    pub effect_detail: Option<MetrolinkTranslatedString>,
}

impl Into<gtfs_realtime::Alert> for NewAlertInner {
    fn into(self) -> gtfs_realtime::Alert {
        gtfs_realtime::Alert {
            active_period: self.active_period.into_iter().map(|x| x.into()).collect(),
            informed_entity: self.informed_entity.into_iter().map(|x| x.into()).collect(),
            cause: self.cause,
            effect: self.effect,
            header_text: self.header_text.map(|x| x.into()),
            description_text: self.description_text.map(|x| x.into()),
            tts_header_text: self.tts_header_text.map(|x| x.into()),
            tts_description_text: self.tts_description_text.map(|x| x.into()),
            url: self.url.map(|x| x.into()),
            severity_level: self.severity_level,
            cause_detail: self.cause_detail.map(|x| x.into()),
            effect_detail: self.effect_detail.map(|x| x.into()),
            image: None,
            image_alternative_text: None,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Advisories {
    #[serde(rename = "Alerts")]
    alerts: NewAlerts,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct NewAlerts {
    #[serde(rename = "PlannedAlerts")]
    planned_alerts: Vec<NewAlert>,
    #[serde(rename = "ServiceAlerts")]
    service_alerts: Vec<NewAlert>,
    #[serde(rename = "BannerAlerts")]
    banner_alerts: Vec<NewAlert>,
}

impl Advisories {
    fn into_gtfs_entity_list(self) -> Vec<gtfs_realtime::FeedEntity> {
        let mut entities = vec![];

        for alert in self.alerts.planned_alerts {
            let entity = gtfs_realtime::FeedEntity {
                id: format!("alert-{}", alert.id),
                is_deleted: None,
                alert: Some(alert.alert.into()),
                ..Default::default()
            };
            entities.push(entity);
        }

        for alert in self.alerts.service_alerts {
            let entity = gtfs_realtime::FeedEntity {
                id: format!("alert-{}", alert.id),
                is_deleted: None,
                alert: Some(alert.alert.into()),
                ..Default::default()
            };
            entities.push(entity);
        }

        for alert in self.alerts.banner_alerts {
            let entity = gtfs_realtime::FeedEntity {
                id: format!("alert-{}", alert.id),
                is_deleted: None,
                alert: Some(alert.alert.into()),
                ..Default::default()
            };
            entities.push(entity);
        }

        entities
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetrolinkTranslatedString {
    #[serde(rename = "Translation")]
    pub translation: Vec<MetrolinkTranslation>,
}

impl Into<gtfs_realtime::TranslatedString> for MetrolinkTranslatedString {
    fn into(self) -> gtfs_realtime::TranslatedString {
        gtfs_realtime::TranslatedString {
            translation: self.translation.into_iter().map(|x| x.into()).collect(),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetrolinkTranslation {
    #[serde(rename = "Text")]
    pub text: String,
    #[serde(rename = "Language")]
    pub language: Option<String>,
}

impl Into<gtfs_realtime::translated_string::Translation> for MetrolinkTranslation {
    fn into(self) -> gtfs_realtime::translated_string::Translation {
        gtfs_realtime::translated_string::Translation {
            text: self.text,
            language: self.language,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewAlertActivePeriod {
    #[serde(rename = "Start")]
    pub start: Option<u64>,
    #[serde(rename = "End")]
    pub end: Option<u64>,
}

impl Into<gtfs_realtime::TimeRange> for NewAlertActivePeriod {
    fn into(self) -> gtfs_realtime::TimeRange {
        gtfs_realtime::TimeRange {
            start: self.start,
            end: self.end,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewAlertInformedEntitySelector {
    #[serde(rename = "AgencyId")]
    pub agency_id: Option<String>,
    #[serde(rename = "RouteId")]
    pub route_id: Option<String>,
    #[serde(rename = "Trip")]
    pub trip: Option<NewAlertTrip>,
    #[serde(rename = "StopId")]
    pub stop_id: Option<String>,
    #[serde(rename = "RouteType")]
    pub route_type: Option<i32>,
    #[serde(rename = " DirectionId")]
    pub direction_id: Option<u32>,
}

impl Into<gtfs_realtime::EntitySelector> for NewAlertInformedEntitySelector {
    fn into(self) -> gtfs_realtime::EntitySelector {
        gtfs_realtime::EntitySelector {
            agency_id: self.agency_id,
            route_id: self.route_id,
            trip: self.trip.map(|x| x.into()),
            stop_id: self.stop_id,
            route_type: self.route_type,
            direction_id: self.direction_id,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewAlertTrip {
    #[serde(rename = "RouteId")]
    pub route_id: Option<String>,
    #[serde(rename = "TripId")]
    pub trip_id: Option<String>,
    #[serde(rename = "DirectionId")]
    pub direction_id: Option<u32>,
    #[serde(rename = "StartTime")]
    pub start_time: Option<String>,
    #[serde(rename = "StartDate")]
    pub start_date: Option<String>,
    #[serde(rename = "ScheduleRelationship")]
    pub schedule_relationship: Option<String>,
}

impl Into<gtfs_realtime::TripDescriptor> for NewAlertTrip {
    fn into(self) -> gtfs_realtime::TripDescriptor {
        gtfs_realtime::TripDescriptor {
            route_id: self.route_id,
            trip_id: self.trip_id,
            direction_id: self.direction_id,
            start_time: self.start_time,
            start_date: self.start_date,
            schedule_relationship: self
                .schedule_relationship
                .map(|x| {
                    gtfs_realtime::trip_descriptor::ScheduleRelationship::from_str_name(&x)
                        .map(|sr| sr as i32)
                })
                .flatten(),
            modified_trip: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RawAlertDetailsPage {
    header: String,
    description: Option<String>,
    start_date: Option<chrono::NaiveDate>,
    end_date: Option<chrono::NaiveDate>,
    route_ids: Vec<String>,
    url: String,
    id: String,
}

fn website_name_to_route_id(route_name: &str) -> Option<&str> {
    match route_name {
        "Antelope Valley Line" => Some("Antelope Valley Line"),
        "Inland Empire-Orange County Line" => Some("Inland Emp.-Orange Co. Line"),
        "Orange County Line" => Some("Orange County Line"),
        "Riverside Line" => Some("Riverside Line"),
        "San Bernardino Line" => Some("San Bernardino Line"),
        "Ventura County Line" => Some("Ventura County Line"),
        "91/Perris Valley Line" => Some("91 Line"),
        _ => None,
    }
}

fn date_string_to_chrono_naive(date_string: &str) -> Option<chrono::NaiveDate> {
    //Remove anything matching "Starting Date: " and "Ending Date: "

    let date_string = date_string
        .replace("Starting Date: ", "")
        .replace("Ending Date: ", "");

    //parse the date string

    chrono::NaiveDate::parse_from_str(&date_string, "%m/%d/%Y").ok()
}

pub async fn fetch_alert_page_data(
    url: &str,
    client: &reqwest::Client,
) -> Result<RawAlertDetailsPage, Box<dyn Error + Send + Sync>> {
    let raw_html = client.get(url).send().await?.text().await?;

    let document = Html::parse_document(&raw_html);

    //look for optional start date at .alertsDetail__date--start

    //look for optional end date at .alertsDetail__date--end

    //header at .h-L alertsDetail__description

    //description at .alertsDetail__details

    //match all routes .alertsDetail__line-station

    let header_selector = scraper::Selector::parse(".h-L.alertsDetail__description").unwrap();
    //pick first element or return error
    let header = document
        .select(&header_selector)
        .next()
        .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, "Header not found"))?
        .text()
        .collect::<String>();

    let description_selector = scraper::Selector::parse(".alertsDetail__details").unwrap();
    let description = document
        .select(&description_selector)
        .next()
        .map(|description| {
            description
                .text()
                .collect::<String>()
                .replace(
                    " For real-time updates, follow us on Facebook and Twitter (X).",
                    "",
                )
                .replace(REMOVE_DELUSION, "")
                .replace(REMOVE_YAP, "")
                .replace("METROLINK ALERTS DETAIL", "")
                .trim()
                .to_string()
        });

    let start_date_selector = scraper::Selector::parse(".alertsDetail__date--start").unwrap();
    //pick first optionally
    let start_date = document
        .select(&start_date_selector)
        .next()
        .map(|x| x.text().collect::<String>());
    let start_date = start_date
        .as_deref()
        .map(date_string_to_chrono_naive)
        .flatten();

    let end_date_selector = scraper::Selector::parse(".alertsDetail__date--end").unwrap();
    //pick first optionally
    let end_date = document
        .select(&end_date_selector)
        .next()
        .map(|x| x.text().collect::<String>());
    let end_date = end_date
        .as_deref()
        .map(date_string_to_chrono_naive)
        .flatten();

    let route_selector = scraper::Selector::parse(".alertsDetail__line-station").unwrap();
    let route_ids = document
        .select(&route_selector)
        .map(|x| x.text().collect::<String>())
        .map(|x| website_name_to_route_id(&x).map(|x| x.to_string()))
        .flatten()
        .collect::<Vec<String>>();

    Ok(RawAlertDetailsPage {
        header,
        description,
        start_date,
        end_date,
        route_ids,
        id: url.replace(ALERT_URL_PREFIX, "").to_string(),
        url: url.to_string(),
    })
}

pub async fn fetch_alerts_from_root_metrolink(
    client: &reqwest::Client,
) -> Result<Vec<RawAlertDetailsPage>, Box<dyn Error + Send + Sync>> {
    //query the root page for text

    //https://metrolinktrains.com/

    let main_page = client
        .get("https://metrolinktrains.com/")
        .send()
        .await?
        .text()
        .await?;

    let document = Html::parse_document(&main_page);

    // look for those big orange banners on the main page
    //document.querySelectorAll(".alert")
    let selector = scraper::Selector::parse(".alert").unwrap();

    let alerts = document.select(&selector);

    //collect hrefs from the alerts

    let mut hrefs = vec![];

    for alert in alerts {
        if let Some(href) = alert.value().attr("href") {
            hrefs.push(format!("https://metrolinktrains.com{}", href));
        }
    }

    drop(document);

    // println!("alerts list: {:?}", hrefs);

    // go to each webpage and get the information about the alerts

    let mut alert_details = vec![];

    for href in hrefs {
        let alert = fetch_alert_page_data(&href, client).await?;
        alert_details.push(alert);
    }

    // println!("alert details: {:?}", alert_details);

    Ok(alert_details)
}

pub fn determine_cause(header: &str, desc: Option<&str>) -> i32 {
    let header_lower = header.to_lowercase();

    if header_lower.contains("construction") {
        return 10;
    }

    if header_lower.contains("medical") {
        return 12;
    }

    if let Some(desc) = desc {
        if desc.to_lowercase().contains("medical") {
            return 12;
        }
    }

    if header_lower.contains("police") {
        return 11;
    }
    if let Some(desc) = desc {
        if desc.to_lowercase().contains("police") {
            return 11;
        }
    }

    if header_lower.contains("maintenance") {
        return 9;
    }

    return 2;
}

pub fn determine_effect(header: &str, desc: Option<&str>) -> i32 {
    if header.to_lowercase().contains("no train service")
        || header.to_lowercase().contains("no service")
    {
        return 1;
    }

    if header.to_lowercase().contains("significant delays") {
        return 2;
    }

    if header.to_lowercase().contains("delay") {
        return 2;
    }

    return 7;
}

pub fn make_en_translated_string(text: String) -> gtfs_realtime::TranslatedString {
    gtfs_realtime::TranslatedString {
        translation: vec![gtfs_realtime::translated_string::Translation {
            text,
            language: Some("en".to_string()),
        }],
    }
}

fn page_to_gtfs_rt_alerts(page: RawAlertDetailsPage) -> gtfs_realtime::FeedEntity {
    let cause = determine_cause(&page.header, page.description.as_ref().map(|x| x.as_str()));
    let effect = determine_effect(&page.header, page.description.as_ref().map(|x| x.as_str()));

    let mut route_ids = vec![];

    for route_id in page.route_ids {
        let route_id = route_id.to_string();
        route_ids.push(route_id);
    }

    let start_date = page.start_date;
    let end_date = page.end_date;

    //convert start_date to 00:00:00 and end_date to 23:59:59, both in Los Angeles time

    let start_dt = start_date
        .map(|x| x.and_hms_opt(0, 0, 0))
        .flatten()
        .map(|x| Pacific.from_local_datetime(&x).single())
        .flatten();

    let end_dt = end_date
        .map(|x| x.and_hms_opt(23, 59, 59))
        .flatten()
        .map(|x| Pacific.from_local_datetime(&x).single())
        .flatten();

    //make timestamps for both start and end

    let start_timestamp: Option<u64> = start_dt.map(|x| x.timestamp() as u64);
    let end_timestamp: Option<u64> = end_dt.map(|x| x.timestamp() as u64);

    gtfs_realtime::FeedEntity {
        id: format!("alert-{}", page.id),
        is_deleted: None,
        alert: Some(gtfs_realtime::Alert {
            cause: Some(cause),
            effect: Some(effect),
            url: Some(make_en_translated_string(page.url)),
            header_text: Some(make_en_translated_string(page.header)),
            description_text: page
                .description
                .as_ref()
                .map(|x| make_en_translated_string(x.to_string())),
            active_period: vec![gtfs_realtime::TimeRange {
                start: start_timestamp,
                end: end_timestamp,
            }],
            informed_entity: route_ids
                .into_iter()
                .map(|route_id| gtfs_realtime::EntitySelector {
                    route_id: Some(route_id),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub async fn gtfs_rt_alerts_from_metrolink_website(
    client: &reqwest::Client,
) -> Result<Vec<gtfs_realtime::FeedEntity>, Box<dyn Error + Send + Sync>> {
    let mut entities = vec![];

    let raw_from_homepage = fetch_alerts_from_root_metrolink(client).await;

    if let Err(raw_from_homepage) = raw_from_homepage {
        eprintln!("Raw from homepage failed");

        return Err(raw_from_homepage);
    }

    let raw_from_homepage = raw_from_homepage.unwrap();

    //conversion to gtfs rt
    let mut finished_id_list = raw_from_homepage.iter().map(|x| x.id.clone()).collect_vec();

    let gtfs_rt_entities_from_homepage = raw_from_homepage
        .into_iter()
        .map(page_to_gtfs_rt_alerts)
        .collect::<Vec<gtfs_realtime::FeedEntity>>();

    entities.extend(gtfs_rt_entities_from_homepage);

    let body_of_alerts = client
        .get(METROLINK_ALERTS_URL)
        .send()
        .await?
        .text()
        .await?;

    let raw_advisories_data = serde_json::from_str::<Advisories>(&body_of_alerts)?;

    let advisories = raw_advisories_data.into_gtfs_entity_list();

    entities.extend(advisories);

    Ok(entities)
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    //to run just this test, type this command
    //cargo test custom_alerts::metrolink_alerts::tests::metrolink_alerts_fetch -- --exact --show-output

    #[tokio::test]
    async fn metrolink_alerts_fetch() {
        //fetch json from url

        let reqwest_client = reqwest::Client::new();

        let test_one_url = fetch_alert_page_data("https://metrolinktrains.com/news/alert-details-page/?alertId=75abe001-9e70-4173-aaef-df8f5ac87ec0", &reqwest_client).await;

        println!("{:#?}", test_one_url);

        let test = gtfs_rt_alerts_from_metrolink_website(&reqwest_client).await;

        println!("{:#?}", test);

        assert!(test.is_ok());
    }
}

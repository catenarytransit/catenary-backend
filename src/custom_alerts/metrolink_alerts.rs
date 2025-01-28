use chrono::{TimeZone, Utc};
use chrono_tz::US::Pacific;
use itertools::Itertools;
use scraper::Html;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::error::Error;
use std::io::ErrorKind;

pub type RawMetrolinkAlerts = Vec<RawMetrolinkEachRoute>;

pub const METROLINK_ALERTS_URL: &str = "https://metrolinktrains.com/advisories/getadvisories?lines=AV&lines=IEOC&lines=OC&lines=RIV&lines=SB&lines=VC&lines=91/PV";

pub const GREETING: &str = "Good morning! We'll be posting your train status updates from 4:00 a.m. to 12:30 a.m. Be safe and have a great day!";

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawMetrolinkEachRoute {
    #[serde(rename = "Line")]
    pub line: String,
    #[serde(rename = "LineAbbreviation")]
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

#[derive(Clone, Debug)]
pub struct RawAlertDetailsPage {
    header: String,
    description: String,
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

    let date = chrono::NaiveDate::parse_from_str(&date_string, "%m/%d/%Y").ok();

    date
}

pub async fn fetch_alert_page_data(
    url: &str,
) -> Result<RawAlertDetailsPage, Box<dyn Error + Send + Sync>> {
    let raw_html = reqwest::get(url).await?.text().await?;

    let document = Html::parse_document(&raw_html);

    //look for optional start date at .alertsDetail__date--start

    //look for optional end date at .alertsDetail__date--end

    //header at .h-L alertsDetail__description

    //description at .alertsDetail__details

    //match all routes .alertsDetail__line-station

    let header_selector = scraper::Selector::parse(".h-L.alertsDetail__description")?;
    //pick first element or return error
    let header = document
        .select(&header_selector)
        .next()
        .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, "Header not found"))?
        .text()
        .collect::<String>();

    let description_selector = scraper::Selector::parse(".alertsDetail__details")?;
    let description = document
        .select(&description_selector)
        .next()
        .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, "Description not found"))?
        .text()
        .collect::<String>()
        .replace(
            " For real-time updates, follow us on Facebook and Twitter (X).",
            "",
        );

    let start_date_selector = scraper::Selector::parse(".alertsDetail__date--start")?;
    //pick first optionally
    let start_date = document
        .select(&start_date_selector)
        .next()
        .map(|x| x.text().collect::<String>());
    let start_date = start_date
        .as_deref()
        .map(date_string_to_chrono_naive)
        .flatten();

    let end_date_selector = scraper::Selector::parse(".alertsDetail__date--end")?;
    //pick first optionally
    let end_date = document
        .select(&end_date_selector)
        .next()
        .map(|x| x.text().collect::<String>());
    let end_date = end_date
        .as_deref()
        .map(date_string_to_chrono_naive)
        .flatten();

    let route_selector = scraper::Selector::parse(".alertsDetail__line-station")?;
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
        id: url
            .replace(
                "https://metrolinktrains.com/news/alert-details-page/?alertId=",
                "",
            )
            .to_string(),
        url: url.to_string(),
    })
}

pub async fn fetch_alerts_from_root_metrolink(
) -> Result<Vec<RawAlertDetailsPage>, Box<dyn Error + Send + Sync>> {
    //query the root page for text

    //https://metrolinktrains.com/

    let main_page = reqwest::get("https://metrolinktrains.com/")
        .await?
        .text()
        .await?;

    let document = Html::parse_document(&main_page);

    // look for those big orange banners on the main page
    //document.querySelectorAll(".alert")
    let selector = scraper::Selector::parse(".alert")?;

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
        let alert = fetch_alert_page_data(&href).await?;
        alert_details.push(alert);
    }

    println!("alert details: {:?}", alert_details);

    Ok(alert_details)
}

pub async fn determine_cause(header: &str, desc: &str) -> i32 {
    let header_lower = header.to_lowercase();

    if header_lower.contains("construction") {
        return 10;
    }

    if header_lower.contains("medical") {
        return 12;
    }

    if desc.to_lowercase().contains("medical") {
        return 12;
    }

    if header_lower.contains("police") {
        return 11;
    }

    if desc.to_lowercase().contains("police") {
        return 11;
    }

    if header_lower.contains("maintenance") {
        return 9;
    }

    return 2;
}

pub async fn determine_effect(header: &str, desc: &str) -> i32 {
    if header.to_lowercase().contains("no train service")
        || header.to_lowercase().contains("no service")
    {
        return 1;
    }

    if header.to_lowercase().contains("significant delays") {
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

pub async fn gtfs_rt_alerts_from_metrolink_website(
) -> Result<Vec<gtfs_realtime::FeedEntity>, Box<dyn Error + Send + Sync>> {
    let mut entities = vec![];

    let raw_from_homepage = fetch_alerts_from_root_metrolink().await?;

    //conversion to gtfs rt
    for raw_homepage_alert in raw_from_homepage {
        let cause =
            determine_cause(&raw_homepage_alert.header, &raw_homepage_alert.description).await;
        let effect =
            determine_effect(&raw_homepage_alert.header, &raw_homepage_alert.description).await;

        let mut route_ids = vec![];

        for route_id in raw_homepage_alert.route_ids {
            let route_id = route_id.to_string();
            route_ids.push(route_id);
        }

        let start_date = raw_homepage_alert.start_date;
        let end_date = raw_homepage_alert.end_date;

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

        entities.push(gtfs_realtime::FeedEntity {
            id: format!("alert-{}", raw_homepage_alert.id),
            is_deleted: None,
            alert: Some(gtfs_realtime::Alert {
                cause: Some(cause),
                effect: Some(effect),
                url: Some(make_en_translated_string(raw_homepage_alert.url)),
                header_text: Some(make_en_translated_string(raw_homepage_alert.header)),
                description_text: Some(make_en_translated_string(raw_homepage_alert.description)),
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
        });
    }

    let body_of_alerts = reqwest::get(METROLINK_ALERTS_URL).await?.text().await?;

    //individual advisories

    let raw_data: RawMetrolinkAlerts = serde_json::from_str(&body_of_alerts)?;

    let raw_data = raw_data
        .into_iter()
        .map(|each_line_alerts| {
            let mut each_line_alerts = each_line_alerts;

            let service_advisories = each_line_alerts
                .service_advisories
                .into_iter()
                .filter(|x| x.message.as_str() != GREETING)
                .collect_vec();

            each_line_alerts.service_advisories = service_advisories;

            each_line_alerts
        })
        .collect_vec();

    //println!("{:#?}", raw_data);

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

        let test = gtfs_rt_alerts_from_metrolink_website().await;

        assert!(test.is_ok());

        println!("{:#?}", test.unwrap());
    }
}

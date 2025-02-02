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

pub const METROLINK_ALERTS_URL: &str = "https://metrolinktrains.com/advisories/getadvisories?lines=AV&lines=IEOC&lines=OC&lines=RIV&lines=SB&lines=VC&lines=91/PV";

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
        .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, "Description not found"))?
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
        .to_string();

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

    println!("alert details: {:?}", alert_details);

    Ok(alert_details)
}

pub fn determine_cause(header: &str, desc: &str) -> i32 {
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

pub fn determine_effect(header: &str, desc: &str) -> i32 {
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
    let cause = determine_cause(&page.header, &page.description);
    let effect = determine_effect(&page.header, &page.description);

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
            description_text: Some(make_en_translated_string(page.description)),
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

    let raw_from_homepage = fetch_alerts_from_root_metrolink(client).await?;

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

    //individual advisories

    let raw_data: RawMetrolinkAlerts = serde_json::from_str(&body_of_alerts)?;

    let raw_data = raw_data
        .into_iter()
        .map(|each_line_alerts| {
            let mut each_line_alerts = each_line_alerts;

            let service_advisories = each_line_alerts
                .service_advisories
                .into_iter()
                .filter(|x| {
                    x.message.as_str() != GREETING && x.message.as_str() != GREETING_WEEKEND
                })
                .collect_vec();

            each_line_alerts.service_advisories = service_advisories;

            each_line_alerts
        })
        .collect_vec();

    let attempt_to_fetch_more_full_alerts = stream::iter(
        raw_data
            .iter()
            .map(|each_route| {
                each_route
                    .service_advisories
                    .iter()
                    .map(|x| x.id.to_owned())
            })
            .flatten()
            //list of alert ids
            // only ids over the number 1000000 have reported been containing text
            .filter(|id_num| *id_num > 1000000)
            .filter(|id_num| !finished_id_list.contains(&id_num.to_string()))
            .map(|id_num| format!("{}{}", ALERT_URL_PREFIX, id_num))
            .map(|url| {
                let url = url.to_owned();
                async move { fetch_alert_page_data(&url, &client).await }
            }),
    )
    .buffer_unordered(10)
    .collect::<Vec<Result<RawAlertDetailsPage, Box<dyn Error + Send + Sync>>>>()
    .await
    .into_iter()
    .filter_map(|x| x.ok())
    .collect::<Vec<RawAlertDetailsPage>>();

    finished_id_list.extend(
        attempt_to_fetch_more_full_alerts
            .iter()
            .map(|x| x.id.clone()),
    );

    let gtfs_rt_entities_from_advisories_page_full_text = attempt_to_fetch_more_full_alerts
        .into_iter()
        .map(page_to_gtfs_rt_alerts)
        .collect::<Vec<gtfs_realtime::FeedEntity>>();

    entities.extend(gtfs_rt_entities_from_advisories_page_full_text);

    let gtfs_rt_entities_from_advisories_page = raw_data
        .into_iter()
        .map(|each_route| {
            let route_id = website_name_to_route_id(&each_route.line).map(|x| x.to_string());

            if let Some(route_id) = route_id {
                Some(
                    each_route
                        .service_advisories
                        .into_iter()
                        .filter(|service_advisory| {
                            !finished_id_list.contains(&service_advisory.id.to_string())
                        })
                        .map(move |service_advisory| FeedEntity {
                            id: format!("alert-{}", service_advisory.id),
                            is_deleted: None,
                            alert: Some(gtfs_realtime::Alert {
                                cause: Some(1),
                                effect: Some(7),
                                url: None,
                                header_text: Some(make_en_translated_string(format!(
                                    "{}: {}",
                                    service_advisory.timestamp, service_advisory.message
                                ))),
                                description_text: None,
                                active_period: vec![gtfs_realtime::TimeRange {
                                    start: None,
                                    end: None,
                                }],
                                informed_entity: vec![gtfs_realtime::EntitySelector {
                                    route_id: Some((&route_id).to_string()),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                )
            } else {
                None
            }
        })
        .flatten()
        .flatten()
        .collect::<Vec<gtfs_realtime::FeedEntity>>();

    entities.extend(gtfs_rt_entities_from_advisories_page);

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

        let reqwest_client = reqwest::Client::new();

        let test = gtfs_rt_alerts_from_metrolink_website(&reqwest_client).await;

        assert!(test.is_ok());

        println!("{:#?}", test.unwrap());
    }
}

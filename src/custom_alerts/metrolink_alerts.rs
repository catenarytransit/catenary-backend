use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use itertools::Itertools;

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

pub fn gtfs_rt_alerts_from_metrolink_website(input: &str) -> anyhow::Result<()> {
    let raw_data:RawMetrolinkAlerts = serde_json::from_str(input)?;

    let raw_data = raw_data.into_iter().map(|each_line_alerts|
        
        {
            let mut each_line_alerts = each_line_alerts;

            let service_advisories = each_line_alerts.service_advisories.into_iter().filter(|x| x.message.as_str() != GREETING).collect_vec();

            each_line_alerts.service_advisories = service_advisories;

            each_line_alerts
        }).collect_vec();

    println!("{:#?}", raw_data);

    Ok(())
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

        let body = reqwest::get(METROLINK_ALERTS_URL)
        .await.unwrap()
        .text()
        .await.unwrap();

        gtfs_rt_alerts_from_metrolink_website(&body).unwrap();


    }
}
// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use ahash::{AHashMap, AHashSet};
use catenary::aspen_dataset::{
    AspenEntitySelector, AspenRawTripInfo, AspenTimeRange, AspenTranslatedString, AspenTranslation,
    AspenisedAlert,
};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use scraper::{Html, Selector};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::Duration;

pub const SNCF_SIRI_SITUATION_EXCHANGE_URL: &str =
    "https://proxy.transport.data.gouv.fr/resource/sncf-siri-lite-situation-exchange";

#[derive(Clone, Debug)]
struct ParsedSituation {
    id: String,
    active_period: Vec<AspenTimeRange>,
    train_numbers: Vec<String>,
    header_text: Option<AspenTranslatedString>,
    description_text: Option<AspenTranslatedString>,
    url: Option<AspenTranslatedString>,
    severity_level: Option<i32>,
}

#[derive(Clone, Copy, Debug)]
struct TagBlock<'a> {
    attributes: &'a str,
    inner: &'a str,
}

/// Fetch SNCF's SIRI Situation Exchange feed and convert it into Aspen alerts.
///
/// SIRI `VehicleJourneyRef` and `TrainNumberRef` values are matched against
/// `trips.txt`'s `trip_short_name`. Every matching static trip_id is included,
/// because the same public train number can be represented by multiple service
/// variants in the static feed.
pub async fn fetch_alerts(
    conn: &mut AsyncPgConnection,
) -> Result<AHashMap<String, AspenisedAlert>, Box<dyn Error + Send + Sync>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent("catenary-aspen-sncf-siri-alerts")
        .build()?;

    let response = client
        .get(SNCF_SIRI_SITUATION_EXCHANGE_URL)
        .send()
        .await?
        .error_for_status()?;
    let xml = response.text().await?;
    let situations = parse_situations(&xml);

    let query_train_numbers = situations
        .iter()
        .flat_map(|situation| situation.train_numbers.iter())
        .flat_map(|train_number| {
            let normalized = normalize_train_number(train_number);
            if normalized == train_number.as_str() {
                vec![train_number.clone()]
            } else {
                vec![train_number.clone(), normalized]
            }
        })
        .filter(|train_number| !train_number.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let matching_trips = if query_train_numbers.is_empty() {
        Vec::new()
    } else {
        catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(catenary::schema::gtfs::trips_compressed::dsl::chateau.eq("sncf"))
            .filter(
                catenary::schema::gtfs::trips_compressed::dsl::trip_short_name
                    .eq_any(&query_train_numbers),
            )
            .load::<catenary::models::CompressedTrip>(conn)
            .await?
    };

    let mut trips_by_train_number: AHashMap<String, Vec<String>> = AHashMap::new();
    for trip in matching_trips {
        let Some(trip_short_name) = trip.trip_short_name.as_deref() else {
            continue;
        };

        trips_by_train_number
            .entry(normalize_train_number(trip_short_name))
            .or_default()
            .push(trip.trip_id);
    }

    for trip_ids in trips_by_train_number.values_mut() {
        trip_ids.sort();
        trip_ids.dedup();
    }

    let mut alerts = AHashMap::new();
    let mut unmatched_train_numbers = BTreeSet::new();

    for situation in situations {
        let mut informed_entity = Vec::new();
        let mut inserted_trip_ids = AHashSet::new();

        for train_number in &situation.train_numbers {
            let normalized = normalize_train_number(train_number);
            let Some(trip_ids) = trips_by_train_number.get(&normalized) else {
                unmatched_train_numbers.insert(train_number.clone());
                continue;
            };

            for trip_id in trip_ids {
                if inserted_trip_ids.insert(trip_id.clone()) {
                    informed_entity.push(trip_selector(trip_id.clone()));
                }
            }
        }

        alerts.insert(
            situation.id,
            AspenisedAlert {
                active_period: situation.active_period,
                informed_entity,
                cause: None,
                effect: None,
                url: situation.url,
                header_text: situation.header_text,
                description_text: situation.description_text,
                tts_header_text: None,
                tts_description_text: None,
                severity_level: situation.severity_level,
                image: None,
                image_alternative_text: None,
                cause_detail: None,
                effect_detail: None,
            },
        );
    }

    if !unmatched_train_numbers.is_empty() {
        let examples = unmatched_train_numbers.iter().take(20).collect::<Vec<_>>();
        println!(
            "SNCF SIRI SX: {} train numbers did not match a static trip_short_name; first examples: {:?}",
            unmatched_train_numbers.len(),
            examples
        );
    }

    Ok(alerts)
}

fn trip_selector(trip_id: String) -> AspenEntitySelector {
    AspenEntitySelector {
        agency_id: None,
        route_id: None,
        route_type: None,
        trip: Some(AspenRawTripInfo {
            trip_id: Some(trip_id),
            route_id: None,
            direction_id: None,
            start_time: None,
            start_date: None,
            schedule_relationship: None,
            modified_trip: None,
        }),
        stop_id: None,
        direction_id: None,
    }
}

fn normalize_train_number(train_number: &str) -> String {
    let trimmed = train_number.trim();
    let normalized = crate::track_number::sncf_siri::normalize_train_num(trimmed);

    if normalized.is_empty() {
        trimmed.to_string()
    } else {
        normalized
    }
}

fn parse_situations(xml: &str) -> Vec<ParsedSituation> {
    find_tag_blocks(xml, "PtSituationElement")
        .into_iter()
        .filter_map(parse_situation)
        .collect()
}

fn parse_situation(block: TagBlock<'_>) -> Option<ParsedSituation> {
    let progress = first_tag_text(block.inner, "Progress")
        .unwrap_or_default()
        .to_ascii_lowercase();
    if matches!(progress.as_str(), "closed" | "cancelled" | "resolved") {
        return None;
    }

    let situation_number = first_tag_text(block.inner, "SituationNumber")?;

    let active_period = find_tag_blocks(block.inner, "ValidityPeriod")
        .into_iter()
        .filter_map(|period| {
            let start = first_tag_text(period.inner, "StartTime")
                .as_deref()
                .and_then(parse_rfc3339_timestamp);
            let end = first_tag_text(period.inner, "EndTime")
                .as_deref()
                .and_then(parse_rfc3339_timestamp);

            if start.is_none() && end.is_none() {
                None
            } else {
                Some(AspenTimeRange { start, end })
            }
        })
        .collect();

    let mut train_numbers = BTreeSet::new();
    for affected_journey in find_tag_blocks(block.inner, "AffectedVehicleJourney") {
        for tag in ["VehicleJourneyRef", "TrainNumberRef"] {
            for reference in find_tag_blocks(affected_journey.inner, tag) {
                let value = plain_text(reference.inner);
                if !value.is_empty() {
                    train_numbers.insert(value);
                }
            }
        }
    }

    let headers = localized_text(block.inner, "Summary");
    let mut descriptions = localized_text(block.inner, "Description");
    merge_localized_text(&mut descriptions, localized_text(block.inner, "Detail"));

    let mut urls = localized_urls(block.inner, "Description");
    merge_localized_urls(&mut urls, localized_urls(block.inner, "Detail"));

    let severity_level = first_tag_text(block.inner, "Severity")
        .as_deref()
        .and_then(gtfs_severity_level);

    Some(ParsedSituation {
        id: format!("sncf-siri-sx:{situation_number}"),
        active_period,
        train_numbers: train_numbers.into_iter().collect(),
        header_text: translated_string(headers),
        description_text: translated_string(descriptions),
        url: translated_urls(urls),
        severity_level,
    })
}

fn find_tag_blocks<'a>(source: &'a str, tag: &str) -> Vec<TagBlock<'a>> {
    let open_prefix = format!("<{tag}");
    let close_tag = format!("</{tag}>");
    let mut blocks = Vec::new();
    let mut cursor = 0;

    while cursor < source.len() {
        let Some(relative_start) = source[cursor..].find(&open_prefix) else {
            break;
        };
        let tag_start = cursor + relative_start;
        let after_name = tag_start + open_prefix.len();

        let valid_boundary = source[after_name..]
            .chars()
            .next()
            .map(|ch| ch == '>' || ch == '/' || ch.is_ascii_whitespace())
            .unwrap_or(false);
        if !valid_boundary {
            cursor = after_name;
            continue;
        }

        let Some(relative_open_end) = source[after_name..].find('>') else {
            break;
        };
        let open_end = after_name + relative_open_end;
        let attributes = &source[after_name..open_end];

        if attributes.trim_end().ends_with('/') {
            cursor = open_end + 1;
            continue;
        }

        let inner_start = open_end + 1;
        let Some(relative_close_start) = source[inner_start..].find(&close_tag) else {
            break;
        };
        let close_start = inner_start + relative_close_start;

        blocks.push(TagBlock {
            attributes,
            inner: &source[inner_start..close_start],
        });
        cursor = close_start + close_tag.len();
    }

    blocks
}

fn first_tag_text(source: &str, tag: &str) -> Option<String> {
    find_tag_blocks(source, tag)
        .into_iter()
        .map(|block| plain_text(block.inner))
        .find(|text| !text.is_empty())
}

fn localized_text(source: &str, tag: &str) -> BTreeMap<Option<String>, Vec<String>> {
    let mut localized = BTreeMap::new();

    for block in find_tag_blocks(source, tag) {
        let text = plain_text(block.inner);
        if text.is_empty() {
            continue;
        }

        let language = extract_attribute(block.attributes, "xml:lang")
            .or_else(|| extract_attribute(block.attributes, "lang"))
            .map(|language| language.to_ascii_lowercase());

        push_unique(localized.entry(language).or_default(), text);
    }

    localized
}

fn merge_localized_text(
    target: &mut BTreeMap<Option<String>, Vec<String>>,
    source: BTreeMap<Option<String>, Vec<String>>,
) {
    for (language, values) in source {
        let target_values = target.entry(language).or_default();
        for value in values {
            push_unique(target_values, value);
        }
    }
}

fn localized_urls(source: &str, tag: &str) -> BTreeMap<Option<String>, String> {
    let link_selector = Selector::parse("a[href]").expect("valid static selector");
    let mut localized = BTreeMap::new();

    for block in find_tag_blocks(source, tag) {
        let language = extract_attribute(block.attributes, "xml:lang")
            .or_else(|| extract_attribute(block.attributes, "lang"))
            .map(|language| language.to_ascii_lowercase());
        let fragment = Html::parse_fragment(block.inner);

        let url = fragment
            .select(&link_selector)
            .filter_map(|link| link.value().attr("href"))
            .find(|href| href.starts_with("https://") || href.starts_with("http://"));

        if let Some(url) = url {
            localized.entry(language).or_insert_with(|| url.to_string());
        }
    }

    localized
}

fn merge_localized_urls(
    target: &mut BTreeMap<Option<String>, String>,
    source: BTreeMap<Option<String>, String>,
) {
    for (language, url) in source {
        target.entry(language).or_insert(url);
    }
}

fn translated_string(
    localized: BTreeMap<Option<String>, Vec<String>>,
) -> Option<AspenTranslatedString> {
    let translation = localized
        .into_iter()
        .filter_map(|(language, values)| {
            let text = values.join("\n\n");
            if text.is_empty() {
                None
            } else {
                Some(AspenTranslation { text, language })
            }
        })
        .collect::<Vec<_>>();

    if translation.is_empty() {
        None
    } else {
        Some(AspenTranslatedString { translation })
    }
}

fn translated_urls(localized: BTreeMap<Option<String>, String>) -> Option<AspenTranslatedString> {
    let translation = localized
        .into_iter()
        .map(|(language, text)| AspenTranslation { text, language })
        .collect::<Vec<_>>();

    if translation.is_empty() {
        None
    } else {
        Some(AspenTranslatedString { translation })
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn plain_text(markup: &str) -> String {
    let fragment = Html::parse_fragment(markup);
    normalize_whitespace(&fragment.root_element().text().collect::<Vec<_>>().join(" "))
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn extract_attribute(attributes: &str, name: &str) -> Option<String> {
    let bytes = attributes.as_bytes();
    let mut cursor = 0;

    while cursor < attributes.len() {
        let relative_start = attributes[cursor..].find(name)?;
        let start = cursor + relative_start;
        let end = start + name.len();

        let valid_start = start == 0 || bytes[start - 1].is_ascii_whitespace();
        let valid_end =
            end == bytes.len() || bytes[end] == b'=' || bytes[end].is_ascii_whitespace();
        if !valid_start || !valid_end {
            cursor = end;
            continue;
        }

        let mut value_start = end;
        while value_start < bytes.len() && bytes[value_start].is_ascii_whitespace() {
            value_start += 1;
        }
        if value_start >= bytes.len() || bytes[value_start] != b'=' {
            cursor = end;
            continue;
        }
        value_start += 1;
        while value_start < bytes.len() && bytes[value_start].is_ascii_whitespace() {
            value_start += 1;
        }
        if value_start >= bytes.len() || !matches!(bytes[value_start], b'\'' | b'"') {
            cursor = end;
            continue;
        }

        let quote = bytes[value_start];
        value_start += 1;
        let relative_value_end = attributes[value_start..]
            .as_bytes()
            .iter()
            .position(|byte| *byte == quote)?;
        let value_end = value_start + relative_value_end;
        return Some(attributes[value_start..value_end].to_string());
    }

    None
}

fn parse_rfc3339_timestamp(value: &str) -> Option<u64> {
    let timestamp = chrono::DateTime::parse_from_rfc3339(value.trim())
        .ok()?
        .timestamp();
    u64::try_from(timestamp).ok()
}

fn gtfs_severity_level(value: &str) -> Option<i32> {
    let normalized = value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();

    match normalized.as_str() {
        "veryslight" | "slight" => Some(2),
        "normal" | "medium" => Some(3),
        "severe" | "verysevere" => Some(4),
        "unknown" => Some(1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multilingual_vehicle_journey_alert() {
        let xml = r#"
            <Siri version="2.0">
              <ServiceDelivery>
                <SituationExchangeDelivery version="2.0">
                  <Situations>
                    <PtSituationElement>
                      <SituationNumber>QOM:Broadcast::3522070212711280410:LOC</SituationNumber>
                      <Progress>published</Progress>
                      <ValidityPeriod>
                        <StartTime>2026-07-13T00:23:00+02:00</StartTime>
                        <EndTime>2026-07-13T14:26:00+02:00</EndTime>
                      </ValidityPeriod>
                      <Severity>slight</Severity>
                      <Summary xml:lang="FR">Attention, certaines gares non desservies</Summary>
                      <Summary xml:lang="EN">Please note: Some stations are not served</Summary>
                      <Description xml:lang="FR">Votre train ne dessert pas certaines gares.</Description>
                      <Detail xml:lang="FR"><p>Cause : matériel immobilisé.</p></Detail>
                      <Detail xml:lang="EN"><p>Reason: equipment is unavailable. <a href="https://example.com/info">Details</a></p></Detail>
                      <Consequences>
                        <Consequence>
                          <Affects>
                            <VehicleJourneys>
                              <AffectedVehicleJourney>
                                <VehicleJourneyRef>14142</VehicleJourneyRef>
                                <TrainNumbers><TrainNumberRef>14142</TrainNumberRef></TrainNumbers>
                              </AffectedVehicleJourney>
                            </VehicleJourneys>
                          </Affects>
                        </Consequence>
                      </Consequences>
                    </PtSituationElement>
                  </Situations>
                </SituationExchangeDelivery>
              </ServiceDelivery>
            </Siri>
        "#;

        let situations = parse_situations(xml);
        assert_eq!(situations.len(), 1);

        let situation = &situations[0];
        assert_eq!(situation.train_numbers, vec!["14142".to_string()]);
        assert_eq!(situation.active_period.len(), 1);
        assert_eq!(situation.severity_level, Some(2));

        let headers = situation.header_text.as_ref().unwrap();
        assert_eq!(headers.translation.len(), 2);
        assert!(headers.translation.iter().any(|translation| {
            translation.language.as_deref() == Some("en")
                && translation.text == "Please note: Some stations are not served"
        }));

        let descriptions = situation.description_text.as_ref().unwrap();
        let french = descriptions
            .translation
            .iter()
            .find(|translation| translation.language.as_deref() == Some("fr"))
            .unwrap();
        assert_eq!(
            french.text,
            "Votre train ne dessert pas certaines gares.\n\nCause : matériel immobilisé."
        );

        let urls = situation.url.as_ref().unwrap();
        assert!(urls.translation.iter().any(|translation| {
            translation.language.as_deref() == Some("en")
                && translation.text == "https://example.com/info"
        }));
    }

    #[test]
    fn excludes_closed_situations() {
        let xml = r#"
            <PtSituationElement>
              <SituationNumber>closed-alert</SituationNumber>
              <Progress>closed</Progress>
              <Summary xml:lang="FR">Closed</Summary>
            </PtSituationElement>
        "#;

        assert!(parse_situations(xml).is_empty());
    }
}

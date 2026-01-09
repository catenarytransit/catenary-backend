// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use ahash::AHashMap;
use catenary::aspen_dataset::AspenisedAlert;
use catenary::convert_text_12h_to_24h;
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref TRANSIT_APP_REGEX: Regex = Regex::new(r"(?i)(the )?transit app").unwrap();
}

/// Cleans alert text for specific chateaus (metrolinktrains, metro~losangeles).
/// - Removes "Please " prefix and capitalizes the next word
/// - Removes "Alert: " prefix
fn clean_alert_text(text: &str, chateau_id: &str) -> String {
    let mut result = text.to_string();

    // Remove "Alert: " prefix if present
    if result.starts_with("Alert: ") {
        result = result.strip_prefix("Alert: ").unwrap().to_string();
    }

    // For metrolinktrains and metro~losangeles, remove "Please " and capitalize next word
    if chateau_id == "metrolinktrains" || chateau_id == "metro~losangeles" {
        if result.starts_with("Please ") {
            let rest = result.strip_prefix("Please ").unwrap();
            // Capitalize the first character of the remaining text
            let mut chars = rest.chars();
            if let Some(first) = chars.next() {
                result = first.to_uppercase().collect::<String>() + chars.as_str();
            } else {
                result = String::new();
            }
        }
    }

    result
}

/// Processes an alert, cleaning text and populating lookup maps.
///
/// Takes an already-converted AspenisedAlert and applies text transformations
/// to the header and description.
pub fn process_alert(
    mut alert: AspenisedAlert,
    alert_id: &str,
    chateau_id: &str,
    impacted_route_id_to_alert_ids: &mut AHashMap<String, Vec<String>>,
    impact_trip_id_to_alert_ids: &mut AHashMap<String, Vec<String>>,
) -> AspenisedAlert {
    // Deduplicate alert content: some agencies repeat the header in the description.
    if alert.header_text.is_some() {
        if alert.header_text == alert.description_text {
            alert.description_text = None;
        }
    }

    if let Some(header_text) = &mut alert.header_text {
        for a in header_text.translation.iter_mut() {
            let cleaned = TRANSIT_APP_REGEX
                .replace_all(&convert_text_12h_to_24h(&a.text), "Catenary Maps")
                .to_string();
            a.text = clean_alert_text(&cleaned, chateau_id);
        }
    }

    if let Some(desc_text) = &mut alert.description_text {
        for a in desc_text.translation.iter_mut() {
            let cleaned = TRANSIT_APP_REGEX
                .replace_all(&convert_text_12h_to_24h(&a.text), "Catenary Maps")
                .to_string()
                .replace("For Real-Time tracking, please visit rt.scmetro.org.", "")
                .replace(
                    "Para el rastreo en tiempo real, por favor visite https://rt.scmetro.org.",
                    "",
                )
                .replace("For real-time tracking, visit https://rt.scmetro.org.", "")
                .replace(
                    "Para seguimiento en tiempo real, visite https://rt.scmetro.org.",
                    "",
                );
            a.text = clean_alert_text(&cleaned, chateau_id);
        }
    }

    for informed_entity in alert.informed_entity.iter() {
        if let Some(route_id) = &informed_entity.route_id {
            impacted_route_id_to_alert_ids
                .entry(route_id.clone())
                .and_modify(|x| x.push(alert_id.to_string()))
                .or_insert(vec![alert_id.to_string()]);
        }

        if let Some(trip) = &informed_entity.trip {
            if let Some(trip_id) = &trip.trip_id {
                impact_trip_id_to_alert_ids
                    .entry(trip_id.clone())
                    .and_modify(|x| x.push(alert_id.to_string()))
                    .or_insert(vec![alert_id.to_string()]);
            }

            if let Some(route_id) = &trip.route_id {
                impacted_route_id_to_alert_ids
                    .entry(route_id.clone())
                    .and_modify(|x| x.push(alert_id.to_string()))
                    .or_insert(vec![alert_id.to_string()]);
            }
        }
    }

    alert
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_alert_text_removes_alert_prefix() {
        let result = clean_alert_text("Alert: Service suspended", "amtrak");
        assert_eq!(result, "Service suspended");
    }

    #[test]
    fn test_clean_alert_text_removes_please_for_metrolink() {
        let result = clean_alert_text("Please check the schedule", "metrolinktrains");
        assert_eq!(result, "Check the schedule");
    }

    #[test]
    fn test_clean_alert_text_removes_please_for_metro_la() {
        let result = clean_alert_text("Please use alternate routes", "metro~losangeles");
        assert_eq!(result, "Use alternate routes");
    }

    #[test]
    fn test_clean_alert_text_keeps_please_for_other_chateaus() {
        let result = clean_alert_text("Please check the schedule", "amtrak");
        assert_eq!(result, "Please check the schedule");
    }

    #[test]
    fn test_clean_alert_text_handles_both() {
        let result = clean_alert_text("Alert: Please stand clear of doors", "metrolinktrains");
        assert_eq!(result, "Stand clear of doors");
    }
}

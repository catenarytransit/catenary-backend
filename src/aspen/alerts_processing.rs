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

/// Processes an alert, cleaning text.
///
/// Takes an already-converted AspenisedAlert and applies text transformations
/// to the header and description.
pub fn process_alert(mut alert: AspenisedAlert, chateau_id: &str) -> AspenisedAlert {
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

    alert
}

pub fn index_alert(
    alert: &AspenisedAlert,
    alert_id: &str,
    impacted_route_id_to_alert_ids: &mut AHashMap<String, Vec<String>>,
    impact_trip_id_to_alert_ids: &mut AHashMap<String, Vec<String>>,
) {
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
}

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn deduplicate_alerts(
    alerts: AHashMap<String, AspenisedAlert>,
) -> AHashMap<String, AspenisedAlert> {
    let mut alerts_by_content_hash: AHashMap<u64, Vec<(String, AspenisedAlert)>> = AHashMap::new();

    for (id, alert) in alerts {
        let mut hasher = DefaultHasher::new();
        alert.header_text.hash(&mut hasher);
        alert.description_text.hash(&mut hasher);
        alert.cause.hash(&mut hasher);
        alert.effect.hash(&mut hasher);
        alert.url.hash(&mut hasher);
        alert.severity_level.hash(&mut hasher);
        alert.image.hash(&mut hasher);
        alert.image_alternative_text.hash(&mut hasher);
        alert.cause_detail.hash(&mut hasher);
        alert.effect_detail.hash(&mut hasher);
        let hash = hasher.finish();

        alerts_by_content_hash
            .entry(hash)
            .or_default()
            .push((id, alert));
    }

    let mut deduplicated_alerts = AHashMap::new();

    for (_, group) in alerts_by_content_hash {
        if group.len() == 1 {
            let (id, alert) = group.into_iter().next().unwrap();
            deduplicated_alerts.insert(id, alert);
            continue;
        }

        // Graph-based merging
        // Nodes are indices in `group`
        // Edges exist if active_period matches OR informed_entity matches
        let n = group.len();
        let mut adjacency: Vec<Vec<usize>> = vec![vec![]; n];

        for i in 0..n {
            for j in (i + 1)..n {
                let (_, alert_a) = &group[i];
                let (_, alert_b) = &group[j];

                // Check for intersection in active periods
                // For exact match requirement (as impl plan said "A.active_period == B.active_period"),
                // but usually deduplication might want overlap?
                // The user request said:
                // - active period is different but the informed entity is the same
                // - the informed entity is different but the active period is the same
                // This implies strict equality check on one field while allowing difference on the other.
                // It does NOT imply intersection. It implies exact match of the set/vector.
                // However, `active_period` is a Vec. Order might matter unless we sort.
                // `informed_entity` is a Vec. Order might matter.
                // Let's assume order independent equality for robustness.

                let active_period_match = {
                    let mut ap_a = alert_a.active_period.clone();
                    ap_a.sort_by(|a, b| a.start.cmp(&b.start));
                    let mut ap_b = alert_b.active_period.clone();
                    ap_b.sort_by(|a, b| a.start.cmp(&b.start));
                    ap_a == ap_b
                };

                let informed_entity_match = {
                    // Sorting informed entities is hard because they contain many optionals.
                    // We can rely on derived Eq if order is typically consistent,
                    // or implement a hashset based check.
                    // Given the context of GTFS-RT, usually order is consistent or strictly defined.
                    // Let's try hashset based for robustness if they are Hash-able.
                    // definied in aspen_dataset.rs, AspenEntitySelector is Hash.
                    let set_a: std::collections::HashSet<_> =
                        alert_a.informed_entity.iter().collect();
                    let set_b: std::collections::HashSet<_> =
                        alert_b.informed_entity.iter().collect();
                    set_a == set_b
                };

                if active_period_match || informed_entity_match {
                    adjacency[i].push(j);
                    adjacency[j].push(i);
                }
            }
        }

        let mut visited = vec![false; n];
        for i in 0..n {
            if visited[i] {
                continue;
            }

            // BFS to find component
            let mut component_indices = Vec::new();
            let mut queue = std::collections::VecDeque::new();
            queue.push_back(i);
            visited[i] = true;

            while let Some(idx) = queue.pop_front() {
                component_indices.push(idx);
                for &neighbor in &adjacency[idx] {
                    if !visited[neighbor] {
                        visited[neighbor] = true;
                        queue.push_back(neighbor);
                    }
                }
            }

            // Merge component
            let mut merged_active_periods = std::collections::HashSet::new();
            let mut merged_informed_entities = std::collections::HashSet::new();

            // Stable ID selection: sort IDs and pick the first one
            let mut ids = Vec::new();

            // We pick the base alert struct from one of them (they are content-identical)
            // but we need to union the periods and entities.
            for &idx in &component_indices {
                let (id, alert) = &group[idx];
                ids.push(id.clone());
                for ap in &alert.active_period {
                    merged_active_periods.insert(ap.clone());
                }
                for ie in &alert.informed_entity {
                    merged_informed_entities.insert(ie.clone());
                }
            }

            ids.sort();
            let primary_id = ids[0].clone();

            // Take the first alert as template
            let (_, mut template_alert) = group[component_indices[0]].clone();

            // Update active periods
            template_alert.active_period = merged_active_periods.into_iter().collect();
            // Sort for determinism
            template_alert
                .active_period
                .sort_by(|a, b| a.start.cmp(&b.start));

            // Update informed entities
            template_alert.informed_entity = merged_informed_entities.into_iter().collect();
            // Sort informed entities? They don't have a clear order.
            // We can leave them as is, or maybe sort by route_id/stop_id for determinism.
            // Let's implement a simple sort key.
            template_alert.informed_entity.sort_by(|a, b| {
                a.route_id
                    .cmp(&b.route_id)
                    .then(a.stop_id.cmp(&b.stop_id))
                    .then(a.triplet_cmp_key().cmp(&b.triplet_cmp_key()))
            });

            deduplicated_alerts.insert(primary_id, template_alert);
        }
    }

    deduplicated_alerts
}

trait TripletKey {
    fn triplet_cmp_key(&self) -> String;
}

impl TripletKey for catenary::aspen_dataset::AspenEntitySelector {
    fn triplet_cmp_key(&self) -> String {
        format!(
            "{:?}{:?}{:?}{:?}",
            self.agency_id,
            self.route_type,
            self.direction_id,
            self.trip.as_ref().map(|t| &t.trip_id)
        )
    }
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

    #[test]
    fn test_deduplicate_alerts_merging() {
        use ahash::AHashMap;
        use catenary::aspen_dataset::{
            AspenEntitySelector, AspenTimeRange, AspenTranslatedString, AspenTranslation,
            AspenisedAlert,
        };

        let make_alert =
            |header: &str, start: u64, end: u64, route_id: Option<&str>| -> AspenisedAlert {
                AspenisedAlert {
                    header_text: Some(AspenTranslatedString {
                        translation: vec![AspenTranslation {
                            text: header.to_string(),
                            language: None,
                        }],
                    }),
                    description_text: None,
                    active_period: vec![AspenTimeRange {
                        start: Some(start),
                        end: Some(end),
                    }],
                    informed_entity: vec![AspenEntitySelector {
                        agency_id: None,
                        route_id: route_id.map(|s| s.to_string()),
                        route_type: None,
                        trip: None,
                        stop_id: None,
                        direction_id: None,
                    }],
                    cause: None,
                    effect: None,
                    url: None,
                    tts_header_text: None,
                    tts_description_text: None,
                    severity_level: None,
                    image: None,
                    image_alternative_text: None,
                    cause_detail: None,
                    effect_detail: None,
                }
            };

        let mut alerts = AHashMap::new();

        // Alert A: Time 1, Route 1
        let a = make_alert("Header", 100, 200, Some("R1"));
        alerts.insert("A".to_string(), a.clone());

        // Alert B: Time 2, Route 1 (Matches A on Route)
        let b = make_alert("Header", 300, 400, Some("R1"));
        alerts.insert("B".to_string(), b.clone());

        // Alert C: Time 2, Route 2 (Matches B on Time)
        let c = make_alert("Header", 300, 400, Some("R2"));
        alerts.insert("C".to_string(), c.clone());

        // Alert D: Time 3, Route 3 (Disconnected)
        let d = make_alert("Header", 500, 600, Some("R3"));
        alerts.insert("D".to_string(), d.clone());

        let deduped = deduplicate_alerts(alerts);

        // Expectation:
        // A, B, C should define a connected component (A-B via Route 1, B-C via Time 2).
        // D is separate.
        // Total alerts: 2.

        assert_eq!(deduped.len(), 2);

        // Find the merged alert (A, B, C)
        // It should have 2 active periods and 2 informed entities.
        let merged_alert = deduped
            .values()
            .find(|x| x.active_period.len() > 1 && x.informed_entity.len() > 1);

        assert!(
            merged_alert.is_some(),
            "Could not find merged alert with multiple periods and entities"
        );
        let merged_alert = merged_alert.unwrap();

        assert_eq!(merged_alert.active_period.len(), 2);
        assert_eq!(merged_alert.informed_entity.len(), 2);
    }
}

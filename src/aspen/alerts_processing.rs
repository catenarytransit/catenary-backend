// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use ahash::{AHashMap, RandomState};
use catenary::aspen_dataset::{
    AspenEntitySelector, AspenRawTripInfo, AspenTimeRange, AspenTranslatedString, AspenTranslation,
    AspenisedAlert,
};
use catenary::convert_text_12h_to_24h;
use compact_str::CompactString;
use dashmap::DashMap;
use lingua::{LanguageDetector, LanguageDetectorBuilder};
use regex::Regex;
use std::cmp::Ordering;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, LazyLock, OnceLock};

const LANGUAGE_DETECTION_CACHE_LIMIT: usize = 16_384;
const METROLINK_WAGON_ALERT_TEXT: &str = "Wagons are not permitted on Metrolink trains because they can block aisles, doorways, and emergency exits, making travel less safe for everyone.";

static LANGUAGE_DETECTOR: LazyLock<LanguageDetector> =
    LazyLock::new(|| LanguageDetectorBuilder::from_all_languages().build());

static LANGUAGE_DETECTION_CACHE: LazyLock<DashMap<String, Option<String>>> =
    LazyLock::new(DashMap::new);

const HEADER_BOILERPLATE: &[&str] = &[
    "Download the Transit App for real-time information.",
    "Download the Transit App for real-time information",
    "Descargue la aplicación Transit para información en tiempo real.",
    "Descargue la aplicación Transit para información en tiempo real",
    "Riders are encouraged to check current routing and stop locations on the Routes & Schedules page, use the Transit App for trip planning and real-time information and alerts, or visit rt.scmetro.org.",
    "Riders are encouraged to check current routing and stop locations on the Routes & Schedules page, use the Transit App for trip planning and real-time information and alerts, or visit rt.scmetro.org",
    "Se recomienda a los pasajeros consultar el recorrido y las paradas actuales en la página de Rutas y Horarios, usar la aplicación Transit para la planificación de viajes y obtener información y alertas en tiempo real, o visitar rt.scmetro.org.",
    "Se recomienda a los pasajeros consultar el recorrido y las paradas actuales en la página de Rutas y Horarios, usar la aplicación Transit para la planificación de viajes y obtener información y alertas en tiempo real, o visitar rt.scmetro.org",
];

const DESCRIPTION_ONLY_BOILERPLATE: &[&str] = &[
    "For Real-Time tracking, please visit rt.scmetro.org.",
    "Para el rastreo en tiempo real, por favor visite https://rt.scmetro.org.",
    "For real-time tracking, visit https://rt.scmetro.org.",
    "Para seguimiento en tiempo real, visite https://rt.scmetro.org.",
];

static HEADER_BOILERPLATE_REGEX: OnceLock<Regex> = OnceLock::new();
static DESCRIPTION_BOILERPLATE_REGEX: OnceLock<Regex> = OnceLock::new();

fn build_removal_regex(patterns: impl IntoIterator<Item = &'static str>) -> Regex {
    // Put longer alternatives first. Several entries differ only by a trailing
    // period, and Rust's regex engine uses leftmost-first matching.
    let mut escaped = patterns
        .into_iter()
        .map(regex::escape)
        .collect::<Vec<String>>();
    escaped.sort_unstable_by_key(|pattern| std::cmp::Reverse(pattern.len()));
    Regex::new(&escaped.join("|")).expect("static alert boilerplate regex must compile")
}

fn header_boilerplate_regex() -> &'static Regex {
    HEADER_BOILERPLATE_REGEX.get_or_init(|| build_removal_regex(HEADER_BOILERPLATE.iter().copied()))
}

fn description_boilerplate_regex() -> &'static Regex {
    DESCRIPTION_BOILERPLATE_REGEX.get_or_init(|| {
        build_removal_regex(
            HEADER_BOILERPLATE
                .iter()
                .copied()
                .chain(DESCRIPTION_ONLY_BOILERPLATE.iter().copied()),
        )
    })
}

/// Remove every match in one regex traversal. If there is no match, return the
/// original allocation unchanged.
fn remove_boilerplate(text: String, matcher: &Regex) -> String {
    let mut matches = matcher.find_iter(&text);
    let first = match matches.next() {
        Some(first) => first,
        None => {
            drop(matches);
            return text;
        }
    };

    let mut cleaned = String::with_capacity(text.len());
    cleaned.push_str(&text[..first.start()]);
    let mut copied_until = first.end();

    for matched in matches {
        cleaned.push_str(&text[copied_until..matched.start()]);
        copied_until = matched.end();
    }

    cleaned.push_str(&text[copied_until..]);
    cleaned
}

/// Cleans alert text for specific chateaus (metrolinktrains, metro~losangeles).
/// - Removes "Please " prefix and capitalizes the next word
/// - Removes "Alert: " prefix
///
/// This owned version avoids allocating another full String when the caller
/// already owns the text buffer.
fn clean_alert_text_owned(mut text: String, chateau_id: &str) -> String {
    if text.starts_with("Alert: ") {
        text.replace_range(.."Alert: ".len(), "");
    }

    if matches!(chateau_id, "metrolinktrains" | "metro~losangeles") && text.starts_with("Please ") {
        text.replace_range(.."Please ".len(), "");

        if let Some(first) = text.chars().next() {
            let first_len = first.len_utf8();
            let uppercase = first.to_uppercase().collect::<String>();
            text.replace_range(..first_len, &uppercase);
        }
    }

    text
}

fn clean_alert_text(text: &str, chateau_id: &str) -> String {
    clean_alert_text_owned(text.to_owned(), chateau_id)
}

fn process_translation(text: &str, chateau_id: &str, matcher: &Regex) -> String {
    // Keep this as one chain: the decoded value must be the input to every
    // subsequent cleanup step. Previously the decode result was overwritten by
    // a later clean_alert_text call.
    let decoded = html_escape::decode_html_entities(text);
    let converted = convert_text_12h_to_24h(decoded.as_ref());
    let cleaned = remove_boilerplate(converted, matcher);
    clean_alert_text_owned(cleaned, chateau_id)
}

/// Detect one language for the complete GTFS translation.
///
/// A GTFS `TranslatedString.translation` entry represents one complete localized
/// string, not a language span inside a larger string. Never split one incoming
/// translation into multiple AspenTranslation values: downstream consumers select
/// a translation by language and are not expected to concatenate them.
fn detect_translation_language(text: &str) -> Option<String> {
    if text.trim().is_empty() {
        return None;
    }

    if let Some(cached) = LANGUAGE_DETECTION_CACHE.get(text) {
        return cached.value().clone();
    }

    let language = LANGUAGE_DETECTOR
        .detect_language_of(text)
        .map(|language| language.iso_code_639_1().to_string());

    if LANGUAGE_DETECTION_CACHE.len() >= LANGUAGE_DETECTION_CACHE_LIMIT {
        LANGUAGE_DETECTION_CACHE.clear();
    }
    LANGUAGE_DETECTION_CACHE.insert(text.to_owned(), language.clone());

    language
}

fn process_translated_string_text(
    translated_string: &mut AspenTranslatedString,
    chateau_id: &str,
    matcher: Option<&Regex>,
) {
    let translations = std::mem::take(&mut translated_string.translation);
    let mut processed = Vec::with_capacity(translations.len());

    for AspenTranslation { text, language } in translations {
        let text = match matcher {
            Some(matcher) => process_translation(&text, chateau_id, matcher),
            None => text,
        };

        processed.push(AspenTranslation { text, language });
    }

    translated_string.translation = processed;
}

fn fill_missing_translation_languages(translated_string: &mut AspenTranslatedString) {
    for translation in &mut translated_string.translation {
        // A language explicitly supplied by the feed is authoritative. Detect
        // only untagged text; this avoids reclassifying proper GTFS translations.
        if translation.language.is_none() {
            translation.language = detect_translation_language(&translation.text);
        }
    }
}

/// When an alert has exactly one header translation and one description
/// translation, treat them as one language-detection unit. Short headers such as
/// "C Line" do not contain enough signal on their own and can otherwise be
/// misclassified even when the description clearly identifies the language.
///
/// Explicit feed-provided language tags remain authoritative. If exactly one of
/// the pair is tagged, that tag is propagated to the untagged translation. If
/// both are untagged, detect the language from `header + description` and assign
/// that same result to both.
fn assign_single_header_description_language(
    header_text: &mut AspenTranslatedString,
    description_text: &mut AspenTranslatedString,
) -> bool {
    if header_text.translation.len() != 1 || description_text.translation.len() != 1 {
        return false;
    }

    let header_language = header_text.translation[0].language.clone();
    let description_language = description_text.translation[0].language.clone();

    let language = match (header_language, description_language) {
        (Some(header_language), Some(description_language)) => {
            // Do not overwrite conflicting language metadata supplied by the
            // source feed. There is nothing left for automatic detection to do.
            if header_language != description_language {
                return true;
            }
            Some(header_language)
        }
        (Some(language), None) | (None, Some(language)) => Some(language),
        (None, None) => {
            let header = &header_text.translation[0].text;
            let description = &description_text.translation[0].text;
            let mut combined = String::with_capacity(header.len() + description.len() + 1);
            combined.push_str(header);
            combined.push('\n');
            combined.push_str(description);
            detect_translation_language(&combined)
        }
    };

    header_text.translation[0].language = language.clone();
    description_text.translation[0].language = language;
    true
}

fn process_translated_string(
    translated_string: &mut AspenTranslatedString,
    chateau_id: &str,
    matcher: Option<&Regex>,
) {
    process_translated_string_text(translated_string, chateau_id, matcher);
    fill_missing_translation_languages(translated_string);
}

pub fn should_drop_alert(alert: &AspenisedAlert, chateau_id: &str) -> bool {
    if chateau_id != "metrolinktrains" {
        return false;
    }

    [
        alert.header_text.as_ref(),
        alert.description_text.as_ref(),
        alert.tts_header_text.as_ref(),
        alert.tts_description_text.as_ref(),
        alert.image_alternative_text.as_ref(),
        alert.cause_detail.as_ref(),
        alert.effect_detail.as_ref(),
    ]
    .into_iter()
    .flatten()
    .flat_map(|translated_string| translated_string.translation.iter())
    .any(|translation| {
        html_escape::decode_html_entities(&translation.text).contains(METROLINK_WAGON_ALERT_TEXT)
    })
}

/// Processes an alert, cleaning text and filling missing language tags.
pub fn process_alert(mut alert: AspenisedAlert, chateau_id: &str) -> AspenisedAlert {
    // Some agencies repeat the header verbatim in the description.
    if alert.header_text.is_some() && alert.header_text == alert.description_text {
        alert.description_text = None;
    }

    // Clean the visible header/description before language detection so the
    // detector sees exactly the text downstream consumers will receive.
    if let Some(header_text) = &mut alert.header_text {
        process_translated_string_text(header_text, chateau_id, Some(header_boilerplate_regex()));
    }

    if let Some(description_text) = &mut alert.description_text {
        process_translated_string_text(
            description_text,
            chateau_id,
            Some(description_boilerplate_regex()),
        );
    }

    let grouped_header_description = match (&mut alert.header_text, &mut alert.description_text) {
        (Some(header_text), Some(description_text)) => {
            assign_single_header_description_language(header_text, description_text)
        }
        _ => false,
    };

    if !grouped_header_description {
        if let Some(header_text) = &mut alert.header_text {
            fill_missing_translation_languages(header_text);
        }
        if let Some(description_text) = &mut alert.description_text {
            fill_missing_translation_languages(description_text);
        }
    }

    if let Some(tts_header_text) = &mut alert.tts_header_text {
        process_translated_string(tts_header_text, chateau_id, None);
    }
    if let Some(tts_description_text) = &mut alert.tts_description_text {
        process_translated_string(tts_description_text, chateau_id, None);
    }
    if let Some(image_alternative_text) = &mut alert.image_alternative_text {
        process_translated_string(image_alternative_text, chateau_id, None);
    }
    if let Some(cause_detail) = &mut alert.cause_detail {
        process_translated_string(cause_detail, chateau_id, None);
    }
    if let Some(effect_detail) = &mut alert.effect_detail {
        process_translated_string(effect_detail, chateau_id, None);
    }

    alert
}

/// Add one alert ID to an index key without inserting the same alert twice.
///
/// `index_alert` processes one alert completely before the next alert is
/// indexed, so a duplicate key for this alert will always see the same alert ID
/// as the last element. This avoids allocating a temporary HashSet per alert.
fn index_one(index: &mut AHashMap<CompactString, Vec<Arc<str>>>, key: &str, alert_id: &Arc<str>) {
    let ids = index.entry(CompactString::new(key)).or_default();

    if !matches!(ids.last(), Some(last) if last.as_ref() == alert_id.as_ref()) {
        ids.push(Arc::clone(alert_id));
    }
}

pub fn index_alert(
    alert: &AspenisedAlert,
    alert_id: &Arc<str>,
    impacted_route_id_to_alert_ids: &mut AHashMap<CompactString, Vec<Arc<str>>>,
    impacted_stop_id_to_alert_ids: &mut AHashMap<CompactString, Vec<Arc<str>>>,
    impact_trip_id_to_alert_ids: &mut AHashMap<CompactString, Vec<Arc<str>>>,
) {
    for informed_entity in &alert.informed_entity {
        if let Some(route_id) = &informed_entity.route_id {
            index_one(impacted_route_id_to_alert_ids, route_id, alert_id);
        }

        if let Some(stop_id) = &informed_entity.stop_id {
            index_one(impacted_stop_id_to_alert_ids, stop_id, alert_id);
        }

        if let Some(trip) = &informed_entity.trip {
            if let Some(trip_id) = &trip.trip_id {
                index_one(impact_trip_id_to_alert_ids, trip_id, alert_id);
            }

            if let Some(route_id) = &trip.route_id {
                index_one(impacted_route_id_to_alert_ids, route_id, alert_id);
            }
        }
    }
}

fn fast_hash<T: Hash + ?Sized>(value: &T, hash_state: &RandomState) -> u64 {
    let mut hasher = hash_state.build_hasher();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Canonicalize active periods once per alert instead of cloning and sorting
/// them for every pairwise comparison.
///
/// Duplicate periods are intentionally retained here to preserve the previous
/// equality semantics. They are removed only after a connected component is
/// merged.
fn canonicalize_active_periods(periods: &mut [AspenTimeRange]) {
    periods.sort_unstable_by(|a, b| a.start.cmp(&b.start).then(a.end.cmp(&b.end)));
}

fn cmp_option_by<T>(
    a: &Option<T>,
    b: &Option<T>,
    compare: impl FnOnce(&T, &T) -> Ordering,
) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => compare(a, b),
    }
}

fn cmp_trip(a: &AspenRawTripInfo, b: &AspenRawTripInfo) -> Ordering {
    a.trip_id
        .cmp(&b.trip_id)
        .then(a.route_id.cmp(&b.route_id))
        .then(a.direction_id.cmp(&b.direction_id))
        .then(a.start_time.cmp(&b.start_time))
        .then(a.start_date.cmp(&b.start_date))
        .then_with(|| {
            cmp_option_by(
                &a.schedule_relationship,
                &b.schedule_relationship,
                |a, b| {
                    catenary::aspen_dataset::schedule_relationship_to_u8(a)
                        .cmp(&catenary::aspen_dataset::schedule_relationship_to_u8(b))
                },
            )
        })
        .then_with(|| {
            cmp_option_by(&a.modified_trip, &b.modified_trip, |a, b| {
                a.modifications_id
                    .cmp(&b.modifications_id)
                    .then(a.affected_trip_id.cmp(&b.affected_trip_id))
            })
        })
}

fn cmp_informed_entity(a: &AspenEntitySelector, b: &AspenEntitySelector) -> Ordering {
    a.agency_id
        .cmp(&b.agency_id)
        .then(a.route_id.cmp(&b.route_id))
        .then(a.route_type.cmp(&b.route_type))
        .then_with(|| cmp_option_by(&a.trip, &b.trip, cmp_trip))
        .then(a.stop_id.cmp(&b.stop_id))
        .then(a.direction_id.cmp(&b.direction_id))
}

/// Canonicalize informed entities as a set.
///
/// AspenEntitySelector implements Hash/Eq but not Ord. The old code allocated
/// formatted Strings from inside the sort comparator. This field-by-field total
/// ordering is allocation-free and deterministic.
fn canonicalize_informed_entities(entities: &mut Vec<AspenEntitySelector>) {
    if entities.len() < 2 {
        return;
    }

    entities.sort_unstable_by(cmp_informed_entity);

    // The old informed-entity comparison used HashSet equality, so duplicates
    // did not affect the matching key.
    entities.dedup();
}

/// Hash the exact same alert-content fields used by the previous implementation.
/// The hash is only an accelerator: same_alert_content() verifies equality, so
/// a 64-bit collision cannot incorrectly merge two different alerts.
fn alert_content_hash(alert: &AspenisedAlert, hash_state: &RandomState) -> u64 {
    let mut hasher = hash_state.build_hasher();
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
    hasher.finish()
}

fn same_alert_content(a: &AspenisedAlert, b: &AspenisedAlert) -> bool {
    a.header_text == b.header_text
        && a.description_text == b.description_text
        && a.cause == b.cause
        && a.effect == b.effect
        && a.url == b.url
        && a.severity_level == b.severity_level
        && a.image == b.image
        && a.image_alternative_text == b.image_alternative_text
        && a.cause_detail == b.cause_detail
        && a.effect_detail == b.effect_detail
}

/// Disjoint-set union / union-find with path compression and union by size.
/// Amortized operations are effectively constant time, O(alpha(n)).
struct DisjointSet {
    parent: Vec<usize>,
    size: Vec<usize>,
}

impl DisjointSet {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
            size: vec![1; len],
        }
    }

    fn find(&mut self, mut node: usize) -> usize {
        let mut root = node;
        while self.parent[root] != root {
            root = self.parent[root];
        }

        while self.parent[node] != node {
            let parent = self.parent[node];
            self.parent[node] = root;
            node = parent;
        }

        root
    }

    fn union(&mut self, a: usize, b: usize) {
        let mut root_a = self.find(a);
        let mut root_b = self.find(b);

        if root_a == root_b {
            return;
        }

        if self.size[root_a] < self.size[root_b] {
            std::mem::swap(&mut root_a, &mut root_b);
        }

        self.parent[root_b] = root_a;
        self.size[root_a] += self.size[root_b];
    }
}

/// Deduplicate content-identical alerts while preserving the previous transitive
/// rule:
///
/// - same content AND same active_period => connected
/// - same content AND same informed_entity => connected
/// - connectivity is transitive across either relation
///
/// The previous implementation explicitly tested every pair in each content
/// bucket and built an adjacency graph. This implementation instead interns the
/// two equality keys and unions each alert with one prior representative of the
/// same key. That changes the graph-discovery work from quadratic to near-linear
/// while preserving connected-component semantics.
pub fn deduplicate_alerts(
    alerts: AHashMap<Arc<str>, AspenisedAlert>,
) -> AHashMap<Arc<str>, AspenisedAlert> {
    if alerts.len() < 2 {
        return alerts;
    }

    let mut entries = alerts.into_iter().collect::<Vec<_>>();
    let hash_state = RandomState::new();

    // Normalize the two graph keys once per alert.
    for (_, alert) in &mut entries {
        canonicalize_active_periods(&mut alert.active_period);
        canonicalize_informed_entities(&mut alert.informed_entity);
    }

    let len = entries.len();

    // Assign a collision-safe integer ID to each unique content value. This
    // keeps the later key maps compact and ensures different alert content can
    // never be unioned even if their 64-bit hashes collide.
    let mut content_group_ids = vec![usize::MAX; len];
    let mut content_hash_representatives: AHashMap<u64, Vec<usize>> = AHashMap::with_capacity(len);
    let mut next_content_group_id = 0usize;

    for i in 0..len {
        let content_hash = alert_content_hash(&entries[i].1, &hash_state);
        let representatives = content_hash_representatives
            .entry(content_hash)
            .or_default();

        let existing_group = representatives.iter().find_map(|&representative| {
            same_alert_content(&entries[i].1, &entries[representative].1)
                .then_some(content_group_ids[representative])
        });

        content_group_ids[i] = if let Some(group_id) = existing_group {
            group_id
        } else {
            let group_id = next_content_group_id;
            next_content_group_id += 1;
            representatives.push(i);
            group_id
        };
    }

    let mut dsu = DisjointSet::new(len);

    // Hash -> candidate representative list. Exact vector equality is still
    // checked, so collisions only cost an extra comparison; they cannot change
    // the result.
    let mut active_period_representatives: AHashMap<(usize, u64), Vec<usize>> =
        AHashMap::with_capacity(len);
    let mut informed_entity_representatives: AHashMap<(usize, u64), Vec<usize>> =
        AHashMap::with_capacity(len);

    for i in 0..len {
        let content_group_id = content_group_ids[i];

        let active_period_hash = fast_hash(entries[i].1.active_period.as_slice(), &hash_state);
        let active_period_candidates = active_period_representatives
            .entry((content_group_id, active_period_hash))
            .or_default();

        if let Some(&representative) = active_period_candidates.iter().find(|&&representative| {
            entries[representative].1.active_period == entries[i].1.active_period
        }) {
            dsu.union(i, representative);
        } else {
            active_period_candidates.push(i);
        }

        let informed_entity_hash = fast_hash(entries[i].1.informed_entity.as_slice(), &hash_state);
        let informed_entity_candidates = informed_entity_representatives
            .entry((content_group_id, informed_entity_hash))
            .or_default();

        if let Some(&representative) = informed_entity_candidates.iter().find(|&&representative| {
            entries[representative].1.informed_entity == entries[i].1.informed_entity
        }) {
            dsu.union(i, representative);
        } else {
            informed_entity_candidates.push(i);
        }
    }

    // Resolve roots before moving entries out of the vector.
    let roots = (0..len).map(|i| dsu.find(i)).collect::<Vec<_>>();
    let mut components: AHashMap<usize, Vec<(Arc<str>, AspenisedAlert)>> =
        AHashMap::with_capacity(len);

    for ((id, alert), root) in entries.into_iter().zip(roots) {
        components.entry(root).or_default().push((id, alert));
    }

    let mut deduplicated_alerts = AHashMap::with_capacity(components.len());

    for mut component in components.into_values() {
        // Stable ID selection without sorting every ID. Use the corresponding
        // alert as the base as well, making selection deterministic.
        let primary_position = component
            .iter()
            .enumerate()
            .min_by(|(_, (id_a, _)), (_, (id_b, _))| id_a.as_ref().cmp(id_b.as_ref()))
            .map(|(position, _)| position)
            .expect("a union-find component cannot be empty");

        let (primary_id, mut merged_alert) = component.swap_remove(primary_position);

        // We own every alert here, so append/move vectors instead of cloning
        // periods and informed entities into temporary hash sets.
        for (_, mut alert) in component {
            merged_alert.active_period.append(&mut alert.active_period);
            merged_alert
                .informed_entity
                .append(&mut alert.informed_entity);
        }

        canonicalize_active_periods(&mut merged_alert.active_period);
        merged_alert.active_period.dedup();
        canonicalize_informed_entities(&mut merged_alert.informed_entity);

        deduplicated_alerts.insert(primary_id, merged_alert);
    }

    deduplicated_alerts
}

#[cfg(test)]
mod tests {
    use super::*;
    use catenary::aspen_dataset::{AspenTranslatedString, AspenTranslation};

    fn make_alert(header: &str, start: u64, end: u64, route_id: Option<&str>) -> AspenisedAlert {
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
                route_id: route_id.map(str::to_string),
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
    }

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
    fn test_process_alert_keeps_decoded_html() {
        let mut alert = make_alert("Alert: Bus &amp; rail service", 100, 200, Some("R1"));
        alert = process_alert(alert, "amtrak");

        assert_eq!(
            alert.header_text.unwrap().translation[0].text,
            "Bus & rail service"
        );
    }

    #[test]
    fn test_process_alert_removes_boilerplate_after_html_decode() {
        let text = "Alert: Riders are encouraged to check current routing and stop locations on the Routes &amp; Schedules page, use the Transit App for trip planning and real-time information and alerts, or visit rt.scmetro.org.";
        let mut alert = make_alert(text, 100, 200, Some("R1"));
        alert = process_alert(alert, "amtrak");

        assert_eq!(alert.header_text.unwrap().translation[0].text, "");
    }

    #[test]
    fn test_html_decode_is_chained_into_metrolink_cleanup() {
        let alert = process_alert(
            make_alert(
                "Alert: Please use Bus &amp; Rail service.",
                100,
                200,
                Some("R1"),
            ),
            "metrolinktrains",
        );

        assert_eq!(
            alert.header_text.unwrap().translation[0].text,
            "Use Bus & Rail service."
        );
    }

    #[test]
    fn test_untagged_translation_is_never_split() {
        let text = "The next train will arrive shortly. This announcement is for all passengers. 日本語でのご案内です。次の電車はまもなく到着します。";
        let alert = process_alert(make_alert(text, 100, 200, Some("R1")), "amtrak");
        let translations = alert.header_text.unwrap().translation;

        // Even if the source accidentally contains more than one language,
        // preserve the GTFS translation as one complete message. A consumer
        // selecting by language must never receive only half of the alert.
        assert_eq!(translations.len(), 1);
        assert_eq!(translations[0].text, text);
    }

    #[test]
    fn test_single_header_and_description_share_combined_language_detection() {
        let mut alert = make_alert("C Line", 100, 200, Some("R1"));
        alert.description_text = Some(AspenTranslatedString {
            translation: vec![AspenTranslation {
                text: "Trains are delayed because of maintenance near the station.".to_string(),
                language: None,
            }],
        });

        let alert = process_alert(alert, "amtrak");
        let header_language = alert.header_text.unwrap().translation[0].language.clone();
        let description_language = alert.description_text.unwrap().translation[0]
            .language
            .clone();

        assert_eq!(header_language, description_language);
        assert_eq!(header_language.as_deref(), Some("en"));
    }

    #[test]
    fn test_single_header_and_description_propagate_explicit_language() {
        let mut alert = make_alert("C Line", 100, 200, Some("R1"));
        alert.description_text = Some(AspenTranslatedString {
            translation: vec![AspenTranslation {
                text: "Trains are delayed because of maintenance near the station.".to_string(),
                language: Some("en".to_string()),
            }],
        });

        let alert = process_alert(alert, "amtrak");

        assert_eq!(
            alert.header_text.unwrap().translation[0]
                .language
                .as_deref(),
            Some("en")
        );
        assert_eq!(
            alert.description_text.unwrap().translation[0]
                .language
                .as_deref(),
            Some("en")
        );
    }

    #[test]
    fn test_language_detection_results_are_cached() {
        let text = "The next train will arrive at the station shortly for all waiting passengers.";
        let first = detect_translation_language(text);
        let second = detect_translation_language(text);

        assert_eq!(first, second);
        assert!(first.is_some());
    }

    #[test]
    fn test_metrolink_wagon_alert_is_dropped() {
        let alert = make_alert(
            &format!("Rider notice: {METROLINK_WAGON_ALERT_TEXT}"),
            100,
            200,
            Some("R1"),
        );

        assert!(should_drop_alert(&alert, "metrolinktrains"));
        assert!(!should_drop_alert(&alert, "amtrak"));
    }

    #[test]
    fn test_deduplicate_alerts_merging() {
        let mut alerts = AHashMap::new();

        // Alert A: Time 1, Route 1
        alerts.insert(Arc::from("A"), make_alert("Header", 100, 200, Some("R1")));

        // Alert B: Time 2, Route 1 (matches A on informed entity)
        alerts.insert(Arc::from("B"), make_alert("Header", 300, 400, Some("R1")));

        // Alert C: Time 2, Route 2 (matches B on active period)
        alerts.insert(Arc::from("C"), make_alert("Header", 300, 400, Some("R2")));

        // Alert D: Time 3, Route 3 (disconnected)
        alerts.insert(Arc::from("D"), make_alert("Header", 500, 600, Some("R3")));

        let deduped = deduplicate_alerts(alerts);

        assert_eq!(deduped.len(), 2);

        let merged_alert = deduped
            .values()
            .find(|alert| alert.active_period.len() > 1 && alert.informed_entity.len() > 1)
            .expect("could not find merged alert");

        assert_eq!(merged_alert.active_period.len(), 2);
        assert_eq!(merged_alert.informed_entity.len(), 2);
    }

    #[test]
    fn test_deduplicate_alerts_active_period_order_is_irrelevant() {
        let mut a = make_alert("Header", 100, 200, Some("R1"));
        a.active_period.push(AspenTimeRange {
            start: Some(300),
            end: Some(400),
        });

        let mut b = make_alert("Header", 300, 400, Some("R2"));
        b.active_period.push(AspenTimeRange {
            start: Some(100),
            end: Some(200),
        });

        let mut alerts = AHashMap::new();
        alerts.insert(Arc::from("A"), a);
        alerts.insert(Arc::from("B"), b);

        let deduped = deduplicate_alerts(alerts);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped.values().next().unwrap().informed_entity.len(), 2);
    }

    #[test]
    fn test_deduplicate_alerts_informed_entity_order_is_irrelevant() {
        let mut a = make_alert("Header", 100, 200, Some("R1"));
        a.informed_entity.push(AspenEntitySelector {
            agency_id: None,
            route_id: Some("R2".to_string()),
            route_type: None,
            trip: None,
            stop_id: None,
            direction_id: None,
        });

        let mut b = make_alert("Header", 300, 400, Some("R2"));
        b.informed_entity.push(AspenEntitySelector {
            agency_id: None,
            route_id: Some("R1".to_string()),
            route_type: None,
            trip: None,
            stop_id: None,
            direction_id: None,
        });

        let mut alerts = AHashMap::new();
        alerts.insert(Arc::from("A"), a);
        alerts.insert(Arc::from("B"), b);

        let deduped = deduplicate_alerts(alerts);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped.values().next().unwrap().active_period.len(), 2);
    }

    #[test]
    fn test_deduplicate_alerts_different_content_does_not_merge() {
        let a = make_alert("Header A", 100, 200, Some("R1"));
        let b = make_alert("Header B", 100, 200, Some("R1"));

        let mut alerts = AHashMap::new();
        alerts.insert(Arc::from("A"), a);
        alerts.insert(Arc::from("B"), b);

        assert_eq!(deduplicate_alerts(alerts).len(), 2);
    }

    #[test]
    fn test_index_alert_avoids_duplicate_route_entries() {
        let mut alert = make_alert("Header", 100, 200, Some("R1"));
        alert.informed_entity[0].trip = Some(AspenRawTripInfo {
            trip_id: Some("T1".to_string()),
            route_id: Some("R1".to_string()),
            direction_id: None,
            start_time: None,
            start_date: None,
            schedule_relationship: None,
            modified_trip: None,
        });
        alert.informed_entity.push(alert.informed_entity[0].clone());

        let alert_id: Arc<str> = Arc::from("A");
        let mut routes = AHashMap::new();
        let mut stops = AHashMap::new();
        let mut trips = AHashMap::new();

        index_alert(&alert, &alert_id, &mut routes, &mut stops, &mut trips);

        assert_eq!(routes.get(&CompactString::new("R1")).unwrap().len(), 1);
        assert_eq!(trips.get(&CompactString::new("T1")).unwrap().len(), 1);
    }
}

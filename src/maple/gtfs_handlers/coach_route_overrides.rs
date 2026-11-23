//! Declarative Maple GTFS coach classification. Loaded per ingest, not compiled in.
use anyhow::{Context, Result};
use gtfs_structures::{Gtfs, RouteType};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct CoachRouteOverrides {
    pub coach_feeds: Vec<String>,
    pub coach_feed_prefixes: Vec<String>,
    pub coach_bus_chateaux: Vec<String>,
    /// Match by GTFS agency_id OR agency_name within this feed.
    pub coach_agencies_by_feed: HashMap<String, Vec<String>>,
}

impl CoachRouteOverrides {
    pub fn load() -> Result<Self> {
        let override_path = std::env::var("COACH_ROUTE_OVERRIDES_PATH").ok();
        let path = override_path.as_deref().unwrap_or("coach-route-overrides.json");
        // An installed Maple executable may run outside the repository root.
        // Use the bundled default only when no explicit config was requested.
        let content = match std::fs::read_to_string(Path::new(path)) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && override_path.is_none() =>
                include_str!("../../../coach-route-overrides.json").to_string(),
            Err(err) => return Err(err).with_context(|| format!("reading coach route overrides from {path}")),
        };
        serde_json::from_str(&content)
            .with_context(|| format!("parsing coach route overrides from {path}"))
    }

    /// Preserve trains (including FlixTrain), metro, ferries and other non-road routes.
    fn is_bus(route_type: &RouteType) -> bool {
        matches!(route_type, RouteType::Bus | RouteType::Coach)
            || matches!(route_type, RouteType::Other(n) if (200..=209).contains(n) || (700..=716).contains(n) || *n == 800)
    }

    pub fn apply(&self, feed_id: &str, chateau: &str, gtfs: &mut Gtfs) -> usize {
        let whole_feed = self.coach_feeds.iter().any(|id| id == feed_id)
            || self.coach_feed_prefixes.iter().any(|prefix| feed_id.starts_with(prefix));
        let chateau_buses = self.coach_bus_chateaux.iter().any(|id| id == chateau);
        let selected = self.coach_agencies_by_feed.get(feed_id);
        let agency_ids = selected.map(|names| {
            gtfs.agencies.iter()
                .filter(|agency| names.iter().any(|name| name == &agency.name || agency.id.as_ref() == Some(name)))
                .filter_map(|agency| agency.id.clone())
                .collect::<HashSet<_>>()
        }).unwrap_or_default();
        let single_agency = (gtfs.agencies.len() == 1)
            .then(|| gtfs.agencies.first())
            .flatten()
            .is_some_and(|agency| selected.is_some_and(|names| names.iter().any(|name|
                name == &agency.name || agency.id.as_ref() == Some(name))));
        let mut changed = 0;
        for route in gtfs.routes.values_mut() {
            if !Self::is_bus(&route.route_type) { continue; }
            let matching_agency = route.agency_id.as_ref().is_some_and(|id| agency_ids.contains(id))
                || (route.agency_id.is_none() && single_agency);
            if whole_feed || chateau_buses || matching_agency {
                if route.route_type != RouteType::Coach {
                    route.route_type = RouteType::Coach;
                    changed += 1;
                }
            }
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_non_bus_modes() {
        assert!(CoachRouteOverrides::is_bus(&RouteType::Bus));
        assert!(CoachRouteOverrides::is_bus(&RouteType::Other(701)));
        assert!(!CoachRouteOverrides::is_bus(&RouteType::Rail));
        assert!(!CoachRouteOverrides::is_bus(&RouteType::Other(100)));
        assert!(!CoachRouteOverrides::is_bus(&RouteType::Ferry));
    }
    #[test]
    fn root_policy_contains_transcor_and_requested_feeds() {
        let config: CoachRouteOverrides =
            serde_json::from_str(include_str!("../../../coach-route-overrides.json")).unwrap();
        assert!(config.coach_feeds.contains(&"f-tds~limousine~express".to_string()));
        assert!(config.coach_feeds.contains(&"f-megabus~us".to_string()));
        assert!(config.coach_feeds.contains(&"f-flixbus~us".to_string()));
        assert!(config.coach_feed_prefixes.contains(&"f-adirondack~trailways".to_string()));
        assert!(config.coach_bus_chateaux.contains(&"amtrak".to_string()));
    }
}

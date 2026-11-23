//! Filtering for informational route-to-route connection suggestions.
//!
//! Coach services are still available through trip, route and journey endpoints;
//! this filter only removes them from nearby/at-stop transfer recommendations.

use std::collections::BTreeMap;

/// GTFS extended route types 200-209 designate coaches and related services.
pub fn is_coach_route_type(route_type: i16) -> bool {
    (200..=209).contains(&route_type)
}

/// Keep transfer route metadata and per-stop route IDs in sync.
///
/// A stop can serve buses, trains and coaches simultaneously, so filtering by
/// `stops.primary_route_type` would hide valid local connections or let coach
/// connections through. Use the actual route's `route_type` instead.
/// Unknown route IDs are dropped because they cannot be rendered as a route.
pub fn exclude_coaches_from_transfer_connections<T>(
    connecting_routes: &mut BTreeMap<String, BTreeMap<String, T>>,
    connections_per_stop: &mut BTreeMap<String, BTreeMap<String, Vec<String>>>,
    route_type: impl Fn(&T) -> i16,
) {
    for routes in connecting_routes.values_mut() {
        routes.retain(|_, route| !is_coach_route_type(route_type(route)));
    }
    connecting_routes.retain(|_, routes| !routes.is_empty());

    for routes_by_chateau in connections_per_stop.values_mut() {
        routes_by_chateau.retain(|chateau, route_ids| {
            let Some(available_routes) = connecting_routes.get(chateau) else {
                return false;
            };
            route_ids.retain(|route_id| available_routes.contains_key(route_id));
            !route_ids.is_empty()
        });
    }
    connections_per_stop.retain(|_, routes| !routes.is_empty());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coach_route_types_cover_the_extended_gtfs_range_only() {
        for route_type in 200..=209 {
            assert!(is_coach_route_type(route_type), "{route_type}");
        }
        for route_type in [0, 1, 2, 3, 4, 11, 199, 210, 700, 800] {
            assert!(!is_coach_route_type(route_type), "{route_type}");
        }
    }

    #[test]
    fn removes_coaches_from_mixed_stops_and_all_route_metadata() {
        let mut routes = BTreeMap::from([
            (
                "mixed-feed".to_string(),
                BTreeMap::from([
                    ("local-bus".to_string(), 3),
                    ("tram".to_string(), 0),
                    ("coach".to_string(), 200),
                    ("express-coach".to_string(), 207),
                    ("other-bus".to_string(), 700),
                ]),
            ),
            (
                "coach-feed".to_string(),
                BTreeMap::from([("long-distance".to_string(), 209)]),
            ),
            (
                "another-feed".to_string(),
                BTreeMap::from([("coach".to_string(), 2)]),
            ),
        ]);
        let mut per_stop = BTreeMap::from([
            (
                "stop-with-mixed-services".to_string(),
                BTreeMap::from([
                    (
                        "mixed-feed".to_string(),
                        vec![
                            "local-bus".into(),
                            "coach".into(),
                            "express-coach".into(),
                            "tram".into(),
                            "other-bus".into(),
                            "unknown".into(),
                        ],
                    ),
                    ("coach-feed".to_string(), vec!["long-distance".into()]),
                    ("another-feed".to_string(), vec!["coach".into()]),
                ]),
            ),
            (
                "coach-only-stop".to_string(),
                BTreeMap::from([("coach-feed".to_string(), vec!["long-distance".into()])]),
            ),
        ]);

        exclude_coaches_from_transfer_connections(&mut routes, &mut per_stop, |route| *route);

        assert_eq!(
            routes["mixed-feed"].keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["local-bus", "other-bus", "tram"]
        );
        assert!(!routes.contains_key("coach-feed"));
        assert_eq!(routes["another-feed"]["coach"], 2);
        assert_eq!(
            per_stop["stop-with-mixed-services"]["mixed-feed"],
            vec!["local-bus", "tram", "other-bus"]
        );
        assert_eq!(
            per_stop["stop-with-mixed-services"]["another-feed"],
            vec!["coach"]
        );
        assert!(!per_stop["stop-with-mixed-services"].contains_key("coach-feed"));
        assert!(!per_stop.contains_key("coach-only-stop"));
    }
}

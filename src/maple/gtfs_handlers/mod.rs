pub mod coach_route_overrides;
pub mod colour_correction;
pub mod convex_hull;
pub mod fix_files;
pub mod flatten;
pub mod gtfs_de_cleanup;
pub mod hull_from_gtfs;
mod ocean_exclusions;
pub mod remove_agencies;
pub mod rename_route_labels;
pub mod route_file_fixer;
pub mod shape_colour_calculator;
pub mod stops_associated_items;

pub const MAPLE_INGESTION_VERSION: i32 = 20;

// Reprocess DfT feeds imported before prefix-based shape matching was enabled.
// Keep unrelated feeds on their existing ingestion version.
pub fn ingestion_version_for_feed(feed_id: &str) -> i32 {
    if crate::gtfs_process::is_dft_uk_static_feed(feed_id) {
        MAPLE_INGESTION_VERSION.max(21)
    } else {
        MAPLE_INGESTION_VERSION
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dft_shape_matching_invalidates_old_ingests_only_for_static_feeds() {
        for id in ["f-bus~dft~gov~uk", "f-bus~dft~gov~uk~kent", "f-bus~dft~gov~uk~greater~london"] {
            assert_eq!(ingestion_version_for_feed(id), MAPLE_INGESTION_VERSION.max(21));
        }
        for id in ["f-bus~dft~gov~uk~rt", "f-nationalrail~uk"] {
            assert_eq!(ingestion_version_for_feed(id), MAPLE_INGESTION_VERSION);
        }
    }
}

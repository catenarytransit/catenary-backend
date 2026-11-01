//! Sources forbidden from regional RAPTOR timetables.
//!
//! This policy applies only to routing, not to GTFS imports, directory pages,
//! or realtime feeds. Check both identifiers so a remapped feed stays excluded.
use crate::routing_common::timetable::ZeroCopyTimetable;
use anyhow::{Context, Result, ensure};

pub fn excluded_routing_source(chateau: &str, feed_id: &str) -> bool {
    matches!(chateau, "flixbus~america" | "flixbus~europe")
        || matches!(feed_id, "f-flixbus~us" | "f-flixbus~eu")
}

/// Validate the actual serialized source table, not only the resolved input.
pub fn validate_routing_sources(timetable: &ZeroCopyTimetable<'_>) -> Result<()> {
    for source in timetable.sources() {
        let chateau = timetable
            .get_string(source.chateau)
            .context("compiled source contains invalid chateau")?;
        let feed_id = timetable
            .get_string(source.onestop_feed_id)
            .context("compiled source contains invalid feed ID")?;
        ensure!(
            !excluded_routing_source(chateau, feed_id),
            "timetable contains excluded routing source {chateau}/{feed_id}; rebuild this region with the updated Gentian, then restart Edelweiss"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::timetable::TimetableOwned;

    #[test]
    fn excludes_both_chateaux_and_feed_ids() {
        assert!(excluded_routing_source("flixbus~america", "some-feed"));
        assert!(excluded_routing_source("flixbus~europe", "some-feed"));
        assert!(excluded_routing_source("renamed-chateau", "f-flixbus~us"));
        assert!(excluded_routing_source("renamed-chateau", "f-flixbus~eu"));
        assert!(!excluded_routing_source("greyhound", "f-other"));
        assert!(!excluded_routing_source("flixbus~americas", "other"));
    }

    #[test]
    fn validates_serialized_source_records() {
        let mut allowed = TimetableOwned::empty(20_000, 1);
        allowed.add_source("other", "other-feed", "attempt").unwrap();
        let encoded = allowed.encode().unwrap();
        let timetable = ZeroCopyTimetable::new(&encoded).unwrap();
        validate_routing_sources(&timetable).unwrap();

        for (chateau, feed) in [
            ("flixbus~america", "anything"),
            ("flixbus~europe", "anything"),
            ("renamed", "f-flixbus~us"),
            ("renamed", "f-flixbus~eu"),
        ] {
            let mut invalid = TimetableOwned::empty(20_000, 1);
            invalid.add_source(chateau, feed, "attempt").unwrap();
            let encoded = invalid.encode().unwrap();
            let timetable = ZeroCopyTimetable::new(&encoded).unwrap();
            assert!(validate_routing_sources(&timetable).is_err());
        }
    }
}

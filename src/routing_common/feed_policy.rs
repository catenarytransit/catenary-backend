//! Routing source policy. All scheduled feeds, including FlixBus, are routable.
//! Per-request coach exclusion is handled by Edelweiss mode_filter.
use crate::routing_common::timetable::ZeroCopyTimetable;
use anyhow::{Context, Result};

/// Retained for Gentian's routing source selection compatibility.
pub fn excluded_routing_source(_chateau: &str, _feed_id: &str) -> bool {
    false
}

/// Check embedded identifiers without denying routable intercity coach sources.
pub fn validate_routing_sources(timetable: &ZeroCopyTimetable<'_>) -> Result<()> {
    for source in timetable.sources() {
        timetable
            .get_string(source.chateau)
            .context("invalid chateau")?;
        timetable
            .get_string(source.onestop_feed_id)
            .context("invalid feed ID")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::timetable::TimetableOwned;

    #[test]
    fn flixbus_is_routable_again() {
        for (chateau, feed) in [
            ("flixbus~america", "f-flixbus~us"),
            ("flixbus~europe", "f-flixbus~eu"),
        ] {
            assert!(!excluded_routing_source(chateau, feed));
            let mut owned = TimetableOwned::empty(20_000, 1);
            owned.add_source(chateau, feed, "attempt").unwrap();
            let encoded = owned.encode().unwrap();
            validate_routing_sources(&ZeroCopyTimetable::new(&encoded).unwrap()).unwrap();
        }
    }
}

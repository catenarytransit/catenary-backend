//! Import directed GTFS stop-pair interchange constraints without rounding.
//!
//! These are *boarding restrictions*, not walking times: they are checked
//! against the last transit alighting event by RAPTOR, so access and egress
//! pedestrian journeys are never charged a station change-time penalty.
use anyhow::{Context, Result, ensure};
use catenary::routing_common::routing_regions::ChateauSource;
use catenary::routing_common::timetable::{
    LocationIdx, StationTransferRule, TimetableOwned,
};
use fallible_iterator::FallibleIterator;
use postgres::{Client, types::ToSql};
use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
pub struct TransferRules {
    rules: BTreeMap<(u32, u32), StationTransferRule>,
}

impl TransferRules {
    pub fn insert(&mut self, from: LocationIdx, to: LocationIdx,
                  kind: i16, min_seconds: Option<u32>) {
        let key = (from.0, to.0);
        if kind == 3 {
            self.rules.insert(key, StationTransferRule {
                from, to, min_seconds: 0, flags: 1,
            });
            return;
        }
        // A prohibition dominates any recommendation for the same stop pair.
        let Some(minimum) = min_seconds else { return };
        let rule = self.rules.entry(key).or_insert(StationTransferRule {
            from, to, min_seconds: 0, flags: 0,
        });
        if !rule.prohibited() {
            rule.min_seconds = rule.min_seconds.max(minimum);
        }
    }

    pub fn finish(self, tt: &mut TimetableOwned) {
        tt.station_transfer_rules.extend(self.rules.into_values());
    }
}

fn locations_for(
    id: LocationIdx,
    tt: &TimetableOwned,
    children: &HashMap<u32, Vec<LocationIdx>>,
) -> Vec<LocationIdx> {
    if tt.location_type[id.idx()] != 2 {
        return vec![id];
    }
    // GTFS transfers can reference a parent station instead of a platform.
    // Expand it to child platforms *within the same source*.
    let mut locations = vec![id];
    if let Some(c) = children.get(&id.0) {
        locations.extend(c.iter().copied());
    }
    locations
}

pub fn load(
    client: &mut Client,
    source: &ChateauSource,
    lookup: &HashMap<String, LocationIdx>,
    tt: &mut TimetableOwned,
    rules: &mut TransferRules,
) -> Result<()> {
    let mut children = HashMap::<u32, Vec<LocationIdx>>::new();
    for &location in lookup.values() {
        let parent = tt.location_parent[location.idx()];
        if parent != LocationIdx::INVALID {
            children.entry(parent.0).or_default().push(location);
        }
    }

    let params: [&(dyn ToSql + Sync); 2] = [&source.onestop_feed_id, &source.attempt_id];
    let mut rows = client.query_raw(
        "SELECT from_stop_id,to_stop_id,transfer_type,min_transfer_seconds,
                from_route_id,to_route_id,from_trip_id,to_trip_id
         FROM gtfs.transfers
         WHERE onestop_feed_id=$1 AND attempt_id=$2",
        params,
    )?;
    let mut scoped = 0u64;
    let mut seated = 0u64;
    let mut imported = 0u64;
    while let Some(row) = rows.next()? {
        let kind: i16 = row.get(2);
        if kind == 4 || kind == 5 {
            // type 4 is through-service semantics, not a pedestrian footpath.
            // type 5 is already honoured by not offering an implicit stay-seated ride.
            seated += 1;
            continue;
        }
        let from_route: Option<String> = row.get(4);
        let to_route: Option<String> = row.get(5);
        let from_trip: Option<String> = row.get(6);
        let to_trip: Option<String> = row.get(7);
        if from_route.is_some() || to_route.is_some() ||
           from_trip.is_some() || to_trip.is_some()
        {
            // Trip/route-scoped constraints need a provenance-aware multi-label
            // RAPTOR extension. Never reinterpret them as global stop rules.
            scoped += 1;
            continue;
        }
        ensure!((0..=3).contains(&kind), "invalid GTFS transfer_type");
        let from_id: String = row.get(0);
        let to_id: String = row.get(1);
        let (Some(&from), Some(&to)) = (lookup.get(&from_id), lookup.get(&to_id))
            else { continue };
        let min: Option<i32> = row.get(3);
        ensure!(min.is_none_or(|seconds| seconds >= 0),
            "negative GTFS transfer time");
        let min = min.map(u32::try_from).transpose()?;
        for a in locations_for(from, tt, &children) {
            for b in locations_for(to, tt, &children) {
                if a == b && kind != 3 {
                    // Nigiri applies explicit same-stop change times in place
                    // of the default two-minute value.
                    if let Some(seconds) = min {
                        tt.location_transfer_seconds[a.idx()] = seconds;
                    }
                }
                rules.insert(a, b, kind, min);
                imported += 1;
            }
        }
    }
    if scoped > 0 || seated > 0 {
        eprintln!(
            "Gentian GTFS transfer import for {}: {} route/trip-specific and {} stay-seated rows require specialized RAPTOR support; not applied as ordinary walking transfers",
            source.onestop_feed_id, scoped, seated,
        );
    }
    eprintln!("Gentian: loaded {imported} source-scoped stop-pair constraints from {}", source.onestop_feed_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transfer_restrictions_are_directed_and_prohibitions_win() {
        let mut p = TransferRules::default();
        p.insert(LocationIdx(1), LocationIdx(2), 2, Some(90));
        p.insert(LocationIdx(1), LocationIdx(2), 2, Some(115));
        p.insert(LocationIdx(2), LocationIdx(1), 0, Some(45));
        p.insert(LocationIdx(1), LocationIdx(2), 3, None);
        p.insert(LocationIdx(1), LocationIdx(2), 2, Some(300));
        let mut tt = TimetableOwned::empty(0, 1);
        p.finish(&mut tt);
        assert_eq!(tt.station_transfer_rules.len(), 2);
        assert_eq!(tt.station_transfer_rules[0].from.0, 1);
        assert_eq!(tt.station_transfer_rules[0].to.0, 2);
        assert!(tt.station_transfer_rules[0].prohibited());
        assert_eq!(tt.station_transfer_rules[1].min_seconds, 45);
    }
}

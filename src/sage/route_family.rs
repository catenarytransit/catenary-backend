use compact_str::CompactString;
use rand::Rng;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct SageRouteFamily {
    pub families: BTreeMap<u32, BTreeSet<CompactString>>,
    pub route_to_family: BTreeMap<CompactString, u32>,
}

impl SageRouteFamily {
    pub fn add_family(&mut self, route_ids: Vec<CompactString>) {
        //make random u32
        let mut rng = rand::rng();
        let family_id: u32 = rng.random();

        //add to families
        self.families
            .insert(family_id, BTreeSet::from_iter(route_ids.iter().cloned()));

        //add to route_to_family

        for route_id in route_ids {
            self.route_to_family.insert(route_id, family_id);
        }
    }
}

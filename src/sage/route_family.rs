use std::collections::{BTreeMap, BTreeSet, HashMap};

use compact_str::CompactString;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SageRouteFamily {
    pub families: BTreeMap<u8, BTreeSet<CompactString>>,
    pub route_to_family: BTreeMap<CompactString, u8>,
}

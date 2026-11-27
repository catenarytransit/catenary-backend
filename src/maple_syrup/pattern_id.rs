//! Version 1 content-addressed pattern IDs. The leading bits are locality
//! hints, not unique agency/route/direction identifiers.
//!
//! direction: [route group:16][direction digest:48]
//! itinerary: [route group:16][direction prefix:16][itinerary digest:32]
//!
//! Hash input framing, field ordering, domain tags, bincode configuration,
//! and bit allocation are part of the persistent ID format. Change the
//! encoding version deliberately if any of them change.

use super::ItineraryCover;
use ahash::AHashMap;
use compact_str::CompactString;
use std::collections::hash_map::Entry;

/// New IDs are distinguishable from legacy decimal u64 strings, and
/// lexicographically sortable in bytewise order.
pub fn encode_pattern_id(id: u64) -> String {
    format!("h{id:016x}")
}

/// Compute a non-unique grouping key independently of the other routes in
/// the feed. The full route ID is still included in both content digests.
pub(super) fn route_group_prefix(
    agency_id: Option<&str>,
    short_name: Option<&str>,
    route_id: &str,
) -> u16 {
    let group = short_name
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(route_id);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"catenary-pattern-route-v1\0");
    for value in [agency_id.unwrap_or(""), group] {
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    u16::from_be_bytes(hasher.finalize().as_bytes()[..2].try_into().unwrap())
}

fn content_digest<T: serde::Serialize>(
    domain: &str,
    feed_id: &str,
    content: &T,
) -> anyhow::Result<[u8; 32]> {
    // Fixed-width, little-endian integers; serde struct field order and
    // length-prefixed strings/sequences; no map iteration or native Hash.
    let bytes =
        bincode::serde::encode_to_vec((domain, feed_id, content), bincode::config::legacy())?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

pub(super) struct PatternIds<'a> {
    // Not part of the prefix or persisted separately in the pattern ID.
    // Namespace the digest because some existing readers key by pattern
    // alone within a chateau, not by (feed, attempt, pattern).
    feed_id: &'a str,
    direction_digests: AHashMap<u64, [u8; 32]>,
}

impl<'a> PatternIds<'a> {
    pub(super) fn new(feed_id: &'a str) -> Self {
        Self {
            feed_id,
            direction_digests: AHashMap::new(),
        }
    }

    pub(super) fn direction(
        &mut self,
        route_prefix: u16,
        route_id: &str,
        stops: Vec<CompactString>,
    ) -> anyhow::Result<u64> {
        // Preserve the existing direction identity: route + ordered stop
        // sequence. Parent-normalised sequences use the very same scheme.
        let digest = content_digest("catenary-direction-v1", self.feed_id, &(route_id, stops))?;
        self.register_direction(route_prefix, digest)
    }

    fn register_direction(&mut self, route_prefix: u16, digest: [u8; 32]) -> anyhow::Result<u64> {
        let suffix = u64::from_be_bytes(digest[..8].try_into().unwrap()) & 0x0000_ffff_ffff_ffff;
        let id = (u64::from(route_prefix) << 48) | suffix;
        match self.direction_digests.entry(id) {
            Entry::Occupied(entry) => {
                anyhow::ensure!(
                    *entry.get() == digest,
                    "direction pattern ID collision in feed {}: {id:016x}",
                    self.feed_id
                );
            }
            Entry::Vacant(entry) => {
                entry.insert(digest);
            }
        }
        Ok(id)
    }

    pub(super) fn itinerary(&self, cover: &ItineraryCover) -> anyhow::Result<u64> {
        // The cover retains all fields of the original itinerary identity,
        // including exact route ID, stop order, offsets and pickup/drop-off.
        let digest = content_digest("catenary-itinerary-v1", self.feed_id, cover)?;
        let suffix = u32::from_be_bytes(digest[..4].try_into().unwrap());
        Ok((cover.direction_pattern_id & 0xffff_ffff_0000_0000) | u64::from(suffix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_preserves_unsigned_order_and_legacy_namespace() {
        assert_eq!(encode_pattern_id(0), "h0000000000000000");
        assert_eq!(encode_pattern_id(u64::MAX), "hffffffffffffffff");
        let ids = [0, 1, 9, 10, 255, 256, u64::MAX / 2, u64::MAX];
        let encoded: Vec<_> = ids.into_iter().map(encode_pattern_id).collect();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn short_names_group_without_erasing_full_route_identity() {
        let prefix = route_group_prefix(Some("agency"), Some("10"), "long-route-a");
        assert_eq!(
            prefix,
            route_group_prefix(Some("agency"), Some(" 10 "), "long-route-b")
        );
        assert_eq!(
            route_group_prefix(Some("agency"), Some(" "), "fallback"),
            route_group_prefix(Some("agency"), None, "fallback")
        );
        let mut ids = PatternIds::new("test-feed");
        let stops = vec!["a".into(), "b".into()];
        let a = ids
            .direction(prefix, "long-route-a", stops.clone())
            .unwrap();
        let b = ids
            .direction(prefix, "long-route-b", stops.clone())
            .unwrap();
        assert_eq!(a >> 48, u64::from(prefix));
        assert_eq!(b >> 48, u64::from(prefix));
        assert_ne!(a, b);
        assert_eq!(a, ids.direction(prefix, "long-route-a", stops).unwrap());
    }

    #[test]
    fn direction_hash_collisions_are_errors_not_replacements() {
        let mut ids = PatternIds::new("test-feed");
        let a = [0u8; 32];
        let mut b = a;
        b[31] = 1;
        let first = ids.register_direction(42, a).unwrap();
        assert!(ids.register_direction(42, b).is_err());
        assert_eq!(ids.register_direction(42, a).unwrap(), first);
    }
}

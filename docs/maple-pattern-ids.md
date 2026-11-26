# Maple pattern identifiers v1

Identifiers remain u64 in the reducer and TEXT in PostgreSQL. New text keys
are h followed by 16 lowercase hexadecimal digits (legacy decimal keys stay
valid until the feed is reimported). No schema rewrite or extra index is
required. Bytewise text order matches unsigned numeric order.

- Direction: 16-bit agency/route grouping prefix + 48-bit digest.
- Itinerary: first 32 bits of direction ID + 32-bit itinerary digest.
- Group prefix uses agency ID and trimmed nonempty route short name; otherwise
  the full route ID. It is intentionally non-unique and independent of other
  routes, agency enumeration order, or import attempt. Short-name changes
  can change the IDs. Full route identity is retained in content digests.
- Feed identity is still stored separately, with no feed prefix bits. The
  feed ID is used only as digest namespace to reduce collisions in existing
  readers keyed by pattern ID alone within a chateau. Attempt ID and spatial
  coordinates are not included.
- Direction identity is route ID plus ordered stop IDs, including repeats.
  Parent direction IDs use the same algorithm with parent-normalised stops.
- Itinerary identity preserves the original ItineraryCover fields, with
  deterministic versioned BLAKE3 and bincode legacy serialization instead
  of Rust Hash/AHash.

The reducer rejects distinct patterns with the same full ID, rather than
silently combining trips. Prefix collisions are allowed. 32-bit itinerary
suffixes have non-negligible birthday collision risk in large direction
groups; on collision the import must fail, not renumber existing patterns.
Large imports reduce agency subsets separately; existing primary keys also
reject duplicate stored pattern IDs in a feed attempt. The digest namespace
does not mathematically guarantee cross-feed uniqueness. Composite consumer
keys and wider IDs remain possible future improvements.

Both normal and large-feed imports sort pattern and compressed-trip groups
by numeric ID. This improves intended locality without another table, but
PostgreSQL may reuse existing free heap pages and does not guarantee physical
order. The large importer orders within its current agency subset. S2/Hilbert
row ordering is separate work.

Generated fallback shapes use the same encoded direction ID in both shape
rows and direction metadata. Supplied GTFS shape IDs are unchanged.

Deploy via normal feed reimports, not an in-place global ID rewrite. Refresh
Aspen static caches and rebuild any affected static timetable snapshots.
Existing attempts remain readable; do not mix new and old ID references
within an attempt. No measured database speedup is claimed.

Validation: cargo test --lib maple_syrup (and build both Maple import paths).

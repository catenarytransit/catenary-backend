# Breaking change: Nigiri-shaped RAPTOR schema (V2)

**Scope:** data model and source-scoped identifiers ONLY. This change does not
compile GTFS route patterns, build service calendars, generate footpaths, or
implement RAPTOR scanning. A schema-only `gentian` executable now writes an
empty but valid V2 file instead of emitting the old, misleading V1 arrays.
The old `src/edelweiss/raptor.rs` implementation remains in the tree as a
reference but is excluded from the active module graph until it is ported.

## Nigiri encodings copied

| Element | Physical encoding | Meaning |
|---|---|---|
| `SourceIdx` | `u16` | data import source |
| `LocationIdx`, `RouteIdx`, `TransportIdx`, `TripIdx` | strong `u32` | separate ID domains |
| `PackedStop` | `u32` | 28-bit location + 4 1-bit boarding/accessibility flags |
| `PackedFootpath` | `u32` | 23-bit target + 9-bit **minutes** |
| `Delta` | `u16` | 5-bit days + 11-bit minute-after-midnight |
| `TrafficDays` | 8 × `u64` | 512 consecutive calendar days |
| route/transport times | columnar, stop-event-major | no per-trip object allocations |

All multi-value relationships use flat typed arrays and compact CSR offsets.
Trip/transport indexes are deliberately separate: a public GTFS trip can map
to multiple internal transports; transports can refer to merged trip sections.
Route flags use the separate route-level and section-level packed bitvectors.

## Multi-feed, château-aware `SourceIdx`

Nigiri has `source_idx_t`, `locations_.src_`, `trip_id_src_`,
`trip_ids_`, `trip_id_strings_`, source-scoped `route_ids_`, and
`trip_id_to_idx_`. Catenary V2 preserves this separation. Every source maps
to one `(chateau, onestop_feed_id, attempt_id)`. A château may have multiple
sources; an external `trip_id`, `stop_id`, `route_id` or `service_id`
cannot be compared across distinct sources.

- `sources: [SourceRecord]` is indexed by `SourceIdx(u16)`.
- `location_source[] + location_id[]` preserves original stop IDs.
- `trip_ids[]` preserves a `(source_idx, raw_trip_id) -> TripIdx`
  mapping for GTFS-RT. One internal trip may have multiple external IDs.
- `route_ids[]` and `trip_public_route_ids[]` preserve source-scoped
  public route identity independent of route patterns.
- Source records embed string-pool references to château/feed/attempt.
  Service/calendar import identity can use the same `SourceIdx` later.

The diagnostic `trip_by_source_id` lookup is linear; building a
query-optimized ID index is deferred to compilation, along with daymask
deduplication and service calendar normalization.

## Binary compatibility

The mmap container is **Catenary V2**, not Cista binary serialization.
It stores a versioned header, 64-bit byte offsets, typed slice counts, and
8-byte-aligned columns. The loader checks version, bounds, UTF-8 strings,
source references, CSR boundaries, route/transport references, and day masks
before giving out borrowed slices. V1 binaries must be recompiled.

## Testing

Run `cargo test --lib routing_common::timetable`. Tests cover the exact
bit widths, day masks spanning 64-bit words, distinct source identities for
identical raw GTFS trip and route IDs, binary round trip, and rejection of
V1/truncated files. The GitHub-hosted patch has not been compiled locally.

## Future PRs (explicitly excluded)

1. Discover sources and source-specific GTFS IDs from PostgreSQL (including
   attempt ID selection); deterministic source registration and service maps.
2. Build 512-day UTC traffic masks with `calendar`, `calendar_dates`,
   timezones/DST, and service-date-to-transport mappings.
3. Group non-overtaking transports by stop pattern, generate event-major
   `Delta` matrices, route masks and reverse location->routes indexes.
4. Port and test RAPTOR scans to the new immutable columns.

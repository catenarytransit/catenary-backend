ALTER TABLE gtfs.routes
    ADD COLUMN url_slug_for_unified_agency TEXT;

-- Route slugs are unique per logical route within a unified agency, but the
-- same logical route may exist in multiple physical feed attempts. Therefore
-- this is intentionally not a UNIQUE index.
CREATE INDEX idx_routes_directory_url_slug
    ON gtfs.routes (url_slug_for_unified_agency, onestop_feed_id, attempt_id)
    INCLUDE (agency_id, chateau, route_id)
    WHERE url_slug_for_unified_agency IS NOT NULL;

-- The directory and slug allocator resolve one unified agency to its exact
-- feed attempt + GTFS agency memberships. Include chateau to keep that lookup
-- index-only in the common case.
CREATE INDEX idx_agencies_route_slug_resolution
    ON gtfs.agencies (unified_agency_id, static_onestop_id, attempt_id, agency_id)
    INCLUDE (chateau)
    WHERE unified_agency_id IS NOT NULL;

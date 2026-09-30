DROP INDEX IF EXISTS gtfs.idx_agencies_route_slug_resolution;
DROP INDEX IF EXISTS gtfs.idx_routes_directory_url_slug;

ALTER TABLE gtfs.routes
    DROP COLUMN IF EXISTS url_slug_for_unified_agency;

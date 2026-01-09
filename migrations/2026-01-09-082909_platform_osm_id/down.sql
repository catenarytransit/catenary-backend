-- Drop index on parent_osm_id
DROP INDEX IF EXISTS gtfs.idx_osm_stations_parent_osm_id;

-- Remove columns from osm_stations
ALTER TABLE gtfs.osm_stations DROP COLUMN parent_osm_id;
ALTER TABLE gtfs.osm_stations DROP COLUMN local_ref;
ALTER TABLE gtfs.osm_stations DROP COLUMN level;

-- Remove osm_platform_id column
ALTER TABLE gtfs.stops DROP COLUMN osm_platform_id;

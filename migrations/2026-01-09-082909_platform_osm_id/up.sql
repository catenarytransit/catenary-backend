-- Add osm_platform_id column for platform-level OSM matches
ALTER TABLE gtfs.stops ADD COLUMN osm_platform_id BIGINT;

-- Add level, local_ref, and parent_osm_id columns to osm_stations for platform identification
ALTER TABLE gtfs.osm_stations ADD COLUMN level TEXT;
ALTER TABLE gtfs.osm_stations ADD COLUMN local_ref TEXT;
ALTER TABLE gtfs.osm_stations ADD COLUMN parent_osm_id BIGINT;

-- Create index on parent_osm_id for efficient lookups
CREATE INDEX idx_osm_stations_parent_osm_id ON gtfs.osm_stations (parent_osm_id);

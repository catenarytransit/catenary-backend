-- Remove new columns from stop_mappings
ALTER TABLE gtfs.stop_mappings 
    DROP COLUMN IF EXISTS osm_station_id,
    DROP COLUMN IF EXISTS osm_station_type,
    DROP COLUMN IF EXISTS osm_import_id;

-- Drop OSM tables
DROP TABLE IF EXISTS gtfs.osm_stations;
DROP TABLE IF EXISTS gtfs.osm_station_imports;

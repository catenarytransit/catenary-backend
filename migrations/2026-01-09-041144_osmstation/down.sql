-- Remove osm_station_id from stops
ALTER TABLE gtfs.stops 
    DROP COLUMN IF EXISTS osm_station_id;

-- Drop OSM tables
DROP TABLE IF EXISTS gtfs.osm_stations;
DROP TABLE IF EXISTS gtfs.osm_station_imports;

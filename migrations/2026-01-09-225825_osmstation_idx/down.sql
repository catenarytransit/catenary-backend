-- Remove OSM station indexes
DROP INDEX IF EXISTS gtfs.idx_stops_osm_station_id;
DROP INDEX IF EXISTS gtfs.idx_osm_stations_osm_id;
